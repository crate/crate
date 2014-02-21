/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.planner;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.analyze.Analysis;
import io.crate.analyze.InsertAnalysis;
import io.crate.analyze.SelectAnalysis;
import io.crate.analyze.UpdateAnalysis;
import io.crate.planner.node.*;
import io.crate.planner.projection.AggregationProjection;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.DefaultTraversalVisitor;
import org.cratedb.Constants;
import org.cratedb.DataType;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.*;

@Singleton
public class Planner extends DefaultTraversalVisitor<Symbol, Analysis> {


    static final PlannerAggregationSplitter splitter = new PlannerAggregationSplitter();
    private static final DataTypeVisitor dataTypeVisitor = new DataTypeVisitor();

    /**
     * dispatch plan creation based on analysis type
     *
     * @param analysis analysis to create plan from
     * @return plan
     */
    public Plan plan(Analysis analysis) {
        Plan plan;
        switch (analysis.type()) {
            case SELECT:
                plan = planSelect((SelectAnalysis) analysis);
                break;
            case INSERT:
                plan = planInsert((InsertAnalysis) analysis);
                break;
            case UPDATE:
                plan = planUpdate((UpdateAnalysis) analysis);
                break;
            default:
                throw new CrateException(String.format("unsupported analysis type '%s'", analysis.type().name()));
        }
        return plan;
    }

    private Plan planSelect(SelectAnalysis analysis) {
        Plan plan = new Plan();
        plan.expectsAffectedRows(false);

        if (analysis.hasGroupBy()) {
            groupBy(analysis, plan);
        } else if (analysis.hasAggregates()) {
            globalAggregates(analysis, plan);
        } else {
            if (analysis.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal()
                    && analysis.table().getRouting(analysis.whereClause()).hasLocations()) {
                if (!analysis.isDelete()) {
                    if (analysis.primaryKeyLiterals() != null
                            && !analysis.primaryKeyLiterals().isEmpty()
                            && !analysis.table().isAlias()) {
                        ESGet(analysis, plan);
                    } else {
                        ESSearch(analysis, plan);
                    }
                } else {
                    if (analysis.primaryKeyLiterals() != null && !analysis.primaryKeyLiterals().isEmpty()) {
                        ESDelete(analysis, plan);
                    } else {
                        ESDeleteByQuery(analysis, plan);
                    }
                }
            } else {
                normalSelect(analysis, plan);
            }
        }
        return plan;
    }

    private void ESDelete(SelectAnalysis analysis, Plan plan) {
        assert analysis.primaryKeyLiterals() != null;
        if (analysis.primaryKeyLiterals().size() > 1) {
            throw new UnsupportedOperationException("Multi column primary keys are currently not supported");
        } else {
            Literal literal = analysis.primaryKeyLiterals().get(0);
            if (literal.symbolType() == SymbolType.SET_LITERAL) {
                // TODO: implement bulk delete task / node
                ESDeleteByQuery(analysis, plan);
            } else {
                plan.add(new ESDeleteNode(
                        analysis.table().ident().name(), literal.valueAsString(), analysis.version()));
                plan.expectsAffectedRows(true);
            }
        }
    }

    private void ESGet(SelectAnalysis analysis, Plan plan) {
        assert analysis.primaryKeyLiterals() != null;

        if (analysis.primaryKeyLiterals().size() > 1) {
            throw new UnsupportedOperationException("Multi column primary keys are currently not supported");
        } else {
            Literal literal = analysis.primaryKeyLiterals().get(0);
            List<String> ids;

            if (literal.symbolType() == SymbolType.SET_LITERAL) {
                Set<Literal> literals = ((SetLiteral) literal).literals();
                ids = new ArrayList<>(literals.size());
                for (Literal id : literals) {
                    ids.add(id.valueAsString());
                }
            } else {
                ids = ImmutableList.of(literal.valueAsString());
            }

            ESGetNode getNode = new ESGetNode(analysis.table().ident().name(), ids);
            getNode.outputs(analysis.outputSymbols());
            plan.add(getNode);
        }
    }

    private void normalSelect(SelectAnalysis analysis, Plan plan) {
        // node or shard level normal select

        // TODO: without locations the localMerge node can be removed and the topN projection
        // added to the collectNode.

        PlannerContextBuilder contextBuilder = new PlannerContextBuilder()
                .output(analysis.outputSymbols())
                .orderBy(analysis.sortSymbols());

        ImmutableList<Projection> projections;
        if (analysis.limit() != null) {
            TopNProjection tnp = new TopNProjection(analysis.limit(), analysis.offset(),
                    analysis.sortSymbols(), analysis.reverseFlags());
            tnp.outputs(contextBuilder.outputs());
            projections = ImmutableList.<Projection>of(tnp);
        } else {
            projections = ImmutableList.<Projection>of();
        }

        CollectNode collectNode = PlanNodeBuilder.collect(analysis, contextBuilder.toCollect(), projections);
        plan.add(collectNode);


        TopNProjection tnp = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                analysis.sortSymbols(),
                analysis.reverseFlags()
        );
        tnp.outputs(contextBuilder.outputs());
        plan.add(PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(tnp), collectNode));
    }

    private void ESSearch(SelectAnalysis analysis, Plan plan) {
        // this is an es query
        // this only supports INFOS as order by
        List<Reference> orderBy;
        if (analysis.isSorted()) {
            orderBy = Lists.transform(analysis.sortSymbols(), new com.google.common.base.Function<Symbol, Reference>() {
                @Override
                public Reference apply(Symbol input) {
                    Preconditions.checkArgument(input.symbolType() == SymbolType.REFERENCE,
                            "Unsupported order symbol for ESPlan", input);
                    return (Reference) input;
                }
            });

        } else {
            orderBy = null;
        }
        ESSearchNode node = new ESSearchNode(
                analysis.outputSymbols(),
                orderBy,
                analysis.reverseFlags(),
                analysis.limit(),
                analysis.offset(),
                analysis.whereClause()
        );
        node.outputTypes(extractDataTypes(analysis.outputSymbols()));
        plan.add(node);
    }

    private void ESDeleteByQuery(Analysis analysis, Plan plan) {
        ESDeleteByQueryNode node = new ESDeleteByQueryNode(
                ImmutableSet.<String>of(analysis.table().ident().name()),
                analysis.whereClause());
        plan.add(node);
        plan.expectsAffectedRows(true);
    }

    private void globalAggregates(SelectAnalysis analysis, Plan plan) {
        // global aggregate: collect and partial aggregate on C and final agg on H
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2)
                .output(analysis.outputSymbols());

        AggregationProjection ap = new AggregationProjection();
        ap.aggregations(contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.collect(
                analysis,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(ap)
        );
        plan.add(collectNode);

        contextBuilder.nextStep();

        //// the hander stuff
        List<Projection> projections = new ArrayList<>();
        projections.add(new AggregationProjection(contextBuilder.aggregations()));

        if (contextBuilder.aggregationsWrappedInScalar) {
            TopNProjection topNProjection = new TopNProjection(1, 0);
            topNProjection.outputs(contextBuilder.outputs());
            projections.add(topNProjection);
        }
        plan.add(PlanNodeBuilder.localMerge(projections, collectNode));
    }

    private void groupBy(SelectAnalysis analysis, Plan plan) {
        if (analysis.rowGranularity().ordinal() < RowGranularity.DOC.ordinal()
                || !analysis.table().getRouting(analysis.whereClause()).hasLocations()) {
            nonDistributedGroupBy(analysis, plan);
        } else {
            distributedGroupBy(analysis, plan);
        }
    }

    private void nonDistributedGroupBy(SelectAnalysis analysis, Plan plan) {
        // TODO:  if the routing is HandlerSideRouting or has no locations
        // the localMergeNode isn't needed but instead the topN projection could be added to the
        // collectNode
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(1, analysis.groupBy())
                .output(analysis.outputSymbols())
                .orderBy(analysis.sortSymbols());

        GroupProjection groupProjection =
                new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.collect(
                analysis,
                contextBuilder.toCollect(),
                ImmutableList.<Projection>of(groupProjection)
        );
        plan.add(collectNode);

        contextBuilder.nextStep();

        // handler
        groupProjection =
                new GroupProjection(contextBuilder.groupBy(), contextBuilder.aggregations());
        TopNProjection topN = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                analysis.sortSymbols(),
                analysis.reverseFlags()
        );
        topN.outputs(contextBuilder.outputs());
        plan.add(PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(groupProjection, topN), collectNode));
    }


    /**
     * distributed collect on mapper nodes
     * with merge on reducer to final (they have row authority)
     * <p/>
     * final merge on handler
     */
    private void distributedGroupBy(SelectAnalysis analysis, Plan plan) {
        PlannerContextBuilder contextBuilder = new PlannerContextBuilder(2, analysis.groupBy())
                .output(analysis.outputSymbols())
                .orderBy(analysis.sortSymbols());

        // collector
        GroupProjection groupProjection = new GroupProjection(
                contextBuilder.groupBy(), contextBuilder.aggregations());
        CollectNode collectNode = PlanNodeBuilder.distributingCollect(
                analysis,
                contextBuilder.toCollect(),
                nodesFromTable(analysis),
                ImmutableList.<Projection>of(groupProjection)
        );
        plan.add(collectNode);

        contextBuilder.nextStep();

        // mergeNode for reducer
        ImmutableList.Builder<Projection> projectionsBuilder = ImmutableList.<Projection>builder();
        projectionsBuilder.add(new GroupProjection(
                contextBuilder.groupBy(),
                contextBuilder.aggregations()));

        boolean topNDone = false;
        if (analysis.limit() != null
                || analysis.offset() > 0
                || contextBuilder.aggregationsWrappedInScalar) {
            topNDone = true;

            TopNProjection topN = new TopNProjection(
                    Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.offset(),
                    0,
                    contextBuilder.orderBy(),
                    analysis.reverseFlags()
            );
            topN.outputs(contextBuilder.outputs());
            projectionsBuilder.add(topN);
        }
        MergeNode mergeNode = PlanNodeBuilder.distributedMerge(collectNode, projectionsBuilder.build());
        plan.add(mergeNode);


        List<Symbol> outputs;
        List<Symbol> orderBy;
        if (topNDone) {
            orderBy = contextBuilder.passThroughOrderBy();
            outputs = contextBuilder.passThroughOutputs();
        } else {
            orderBy = contextBuilder.orderBy();
            outputs = contextBuilder.outputs();
        }
        // mergeNode handler
        TopNProjection topN = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                orderBy,
                analysis.reverseFlags()
        );
        topN.outputs(outputs);
        MergeNode localMergeNode = PlanNodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode);
        plan.add(localMergeNode);
    }

    private List<String> nodesFromTable(SelectAnalysis analysis) {
        return Lists.newArrayList(analysis.table().getRouting(analysis.whereClause()).nodes());
    }

    private Plan planInsert(InsertAnalysis analysis) {
        Preconditions.checkState(analysis.values().size() >= 1, "no values given");
        Plan plan = new Plan();
        ESIndex(analysis, plan);
        return plan;
    }

    private void ESIndex(InsertAnalysis analysis, Plan plan) {
        String index = analysis.table().ident().name();
        ESIndexNode indexNode = new ESIndexNode(index,
                analysis.columns(),
                analysis.values(),
                analysis.primaryKeyColumnIndices().toArray());
        plan.add(indexNode);
        plan.expectsAffectedRows(true);
    }

    private Plan planUpdate(UpdateAnalysis analysis) {
        Plan plan = new Plan();

        ESUpdateNode node = new ESUpdateNode(
                analysis.table().ident().name(),
                analysis.assignments(),
                analysis.whereClause(),
                analysis.version(),
                analysis.primaryKeyLiterals()
        );
        plan.add(node);
        plan.expectsAffectedRows(true);
        return plan;
    }

    static List<DataType> extractDataTypes(List<Symbol> symbols) {
        List<DataType> types = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            types.add(symbol.accept(dataTypeVisitor, null));
        }
        return types;
    }

    static List<DataType> extractDataTypes(List<Projection> projections, @Nullable List<DataType> inputTypes) {
        int projectionIdx = projections.size() - 1;
        Projection lastProjection = projections.get(projectionIdx);
        List<DataType> types = new ArrayList<>(lastProjection.outputs().size());

        List<DataType> dataTypes = Objects.firstNonNull(inputTypes, ImmutableList.<DataType>of());

        for (int c = 0; c < lastProjection.outputs().size(); c++) {
            types.add(resolveType(projections, projectionIdx, c, dataTypes));
        }

        return types;
    }

    private static DataType resolveType(List<Projection> projections, int projectionIdx, int columnIdx, List<DataType> inputTypes) {
        Projection projection = projections.get(projectionIdx);
        Symbol symbol = projection.outputs().get(columnIdx);
        DataType type = symbol.accept(dataTypeVisitor, null);
        if (type == null) {
            if (projectionIdx > 0) {
                return resolveType(projections, projectionIdx - 1, columnIdx, inputTypes);
            } else {
                assert symbol instanceof InputColumn; // otherwise type shouldn't be null
                return inputTypes.get(((InputColumn) symbol).index());
            }
        }

        return type;
    }

}
