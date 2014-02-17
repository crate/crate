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


    static class Context {

        private final int numGroupKeys;
        private Map<Symbol, InputColumn> symbols = new LinkedHashMap<>();
        private Map<Aggregation, InputColumn> aggregations = new LinkedHashMap<>();
        private Aggregation.Step toAggStep;

        Context(Aggregation.Step toAggStep) {
            this.toAggStep = toAggStep;
            this.numGroupKeys = 0;
        }

        Context(Aggregation.Step toAggStep, int numGroupKeys) {
            this.toAggStep = toAggStep;
            this.numGroupKeys = numGroupKeys;
        }

        public InputColumn allocateSymbol(Symbol symbol) {
            InputColumn ic = symbols.get(symbol);
            if (ic == null) {
                // TODO: add returnType to inputColumn
                // would simplify PlanNodeStreamerVisitor and type resolving
                ic = new InputColumn(symbols.size());
                symbols.put(symbol, ic);
            }

            return ic;
        }


        public Symbol allocateAggregation(Aggregation aggregation) {
            InputColumn ic = aggregations.get(aggregation);
            if (ic == null) {
                ic = new InputColumn(numGroupKeys + aggregations.size());
                aggregations.put(aggregation, ic);
            }
            return ic;
        }

        public Symbol allocateAggregation(Function function) {
            if (toAggStep == null) {
                return function;
            }
            Aggregation agg = new Aggregation(function.info(), function.arguments(),
                    Aggregation.Step.ITER, toAggStep);

            Symbol ic = allocateAggregation(agg);
            // split the aggregation if we are not final
            if (toAggStep == Aggregation.Step.FINAL) {
                return ic;
            } else {
                return new Aggregation(function.info(), ImmutableList.<Symbol>of(ic),
                        toAggStep, Aggregation.Step.FINAL);
            }
        }

        public List<Symbol> symbolList() {
            List<Symbol> result = new ArrayList<>(symbols.size());
            for (Symbol symbol : symbols.keySet()) {
                result.add(symbol);
            }
            return result;
        }

        public List<Aggregation> aggregationList() {
            List<Aggregation> result = new ArrayList<>(aggregations.size());
            for (Aggregation symbol : aggregations.keySet()) {
                result.add(symbol);
            }
            return result;
        }
    }

    static class SplittingPlanNodeVisitor extends SymbolVisitor<Context, Symbol> {

        @Override
        protected Symbol visitSymbol(Symbol symbol, Context context) {
            return context.allocateSymbol(symbol);
        }

        @Override
        public Symbol visitInputColumn(InputColumn inputColumn, Context context) {
            // override to make sure we do not replace a symbol twice
            return inputColumn;
        }

        public void process(List<Symbol> symbols, Context context) {
            if (symbols != null) {
                for (int i = 0; i < symbols.size(); i++) {
                    symbols.set(i, process(symbols.get(i), context));
                }
            }
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            return super.visitReference(symbol, context);
        }

        @Override
        public Symbol visitAggregation(Aggregation symbol, Context context) {
            return context.allocateAggregation(symbol);
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            if (function.info().isAggregate()) {
                // split off below aggregates
                for (int i = 0; i < function.arguments().size(); i++) {
                    Symbol ic = process(function.arguments().get(i), context);
                    // note, that this modifies the tree
                    function.arguments().set(i, ic);
                }
                return context.allocateAggregation(function);
            } else {
                return context.allocateSymbol(function);
            }
        }
    }

    private static final SplittingPlanNodeVisitor nodeVisitor = new SplittingPlanNodeVisitor();
    private static final DataTypeVisitor dataTypeVisitor = new DataTypeVisitor();

    private static class NodeBuilder {

        static CollectNode distributingCollect(Analysis analysis,
                                               List<Symbol> toCollect,
                                               List<String> downstreamNodes,
                                               ImmutableList<Projection> projections) {
            CollectNode node = new CollectNode("distributing collect", analysis.table().getRouting(analysis.whereClause()));
            node.whereClause(analysis.whereClause());
            node.maxRowGranularity(analysis.rowGranularity());
            node.downStreamNodes(downstreamNodes);
            node.toCollect(toCollect);
            node.projections(projections);

            setOutputTypes(node);
            return node;
        }

        static MergeNode distributedMerge(CollectNode collectNode,
                                          ImmutableList<Projection> projections) {
            MergeNode node = new MergeNode("distributed merge", collectNode.executionNodes().size());
            node.projections(projections);

            // TODO: all nodes by default?
            node.executionNodes(collectNode.routing().nodes());

            connectTypes(collectNode, node);
            return node;
        }

        static MergeNode localMerge(ImmutableList<Projection> projections,
                                    PlanNode previousNode) {
            MergeNode node = new MergeNode("localMerge", previousNode.executionNodes().size());
            node.projections(projections);
            connectTypes(previousNode, node);
            return node;
        }

        /**
         * calculates the outputTypes using the projections and input types.
         * must be called after projections have been set.
         */
        static void setOutputTypes(CollectNode node) {
            if (node.projections().isEmpty()) {
                node.outputTypes(extractDataTypes(node.toCollect()));
            } else {
                node.outputTypes(extractDataTypes(node.projections(), extractDataTypes(node.toCollect())));
            }
        }

        /**
         * sets the inputTypes from the previousNode's outputTypes
         * and calculates the outputTypes using the projections and input types.
         *
         * must be called after projections have been set
         */
        static void connectTypes(PlanNode previousNode, MergeNode nextNode) {
            nextNode.inputTypes(previousNode.outputTypes());
            nextNode.outputTypes(extractDataTypes(nextNode.projections(), nextNode.inputTypes()));
        }

        static CollectNode collect(Analysis analysis,
                                   List<Symbol> toCollect,
                                   ImmutableList<Projection> projections) {
            CollectNode node = new CollectNode("collect", analysis.table().getRouting(analysis.whereClause()));
            node.whereClause(analysis.whereClause());
            node.toCollect(toCollect);
            node.maxRowGranularity(analysis.rowGranularity());
            node.projections(projections);

            setOutputTypes(node);
            return node;
        }
    }

    /**
     * dispatch plan creation based on analysis type
     * @param analysis analysis to create plan from
     * @return plan
     */
    public Plan plan(Analysis analysis) {
        Plan plan;
        switch(analysis.type()) {
            case SELECT:
                plan = planSelect((SelectAnalysis)analysis);
                break;
            case INSERT:
                plan = planInsert((InsertAnalysis)analysis);
                break;
            default:
                throw new CrateException(String.format("unsupported analysis type '%s'", analysis.type().name()));
        }
        return plan;
    }

    private Plan planSelect(SelectAnalysis analysis) {
        Plan plan = new Plan();

        if (analysis.hasGroupBy()) {
            groupBy(analysis, plan);
        } else {
            if (analysis.hasAggregates()) {
                globalAggregates(analysis, plan);
            } else {
                if (analysis.rowGranularity().ordinal() >= RowGranularity.DOC.ordinal()) {
                    if (!analysis.isDelete()) {
                        if (analysis.primaryKeyLiterals() != null && !analysis.primaryKeyLiterals().isEmpty()) {
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
        }
        plan.expectsAffectedRows(false);
        return plan;
    }

    private void ESDelete(SelectAnalysis analysis, Plan plan) {
        assert analysis.primaryKeyLiterals() != null;
        if (analysis.primaryKeyLiterals().size() > 1) {
            throw new UnsupportedOperationException("Multi column primary keys are currently not supported");
        } else {
            Literal literal = analysis.primaryKeyLiterals().get(0);
            if (literal.symbolType() == SymbolType.SET_LITERAL) {
                throw new UnsupportedOperationException("Don't know how to plan a multi delete yet");
            } else {
                plan.add(new ESDeleteNode(analysis.table().ident().name(), literal.value().toString()));
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
                Set<?> objects = ((SetLiteral) literal).value();
                ids = new ArrayList<>(objects.size());
                for (Object object : objects) {
                    ids.add(object.toString());
                }
            } else {
                ids = Arrays.asList(literal.value().toString());
            }

            ESGetNode getNode = new ESGetNode(analysis.table().ident().name(), ids);
            getNode.outputs(analysis.outputSymbols());
            plan.add(getNode);
        }
    }

    private void normalSelect(SelectAnalysis analysis, Plan plan) {
        // node or shard level normal select
        Context context = new Context(null);
        nodeVisitor.process(analysis.outputSymbols(), context);
        nodeVisitor.process(analysis.sortSymbols(), context);

        ImmutableList<Projection> projections;
        if (analysis.limit() != null) {
            TopNProjection tnp = new TopNProjection(analysis.limit(), analysis.offset(),
                    analysis.sortSymbols(), analysis.reverseFlags());
            tnp.outputs(analysis.outputSymbols());
            projections = ImmutableList.<Projection>of(tnp);
        } else {
            projections = ImmutableList.<Projection>of();
        }

        CollectNode collectNode =
                NodeBuilder.collect(analysis, context.symbolList(), projections);
        plan.add(collectNode);

        nodeVisitor.process(analysis.outputSymbols(), context);
        nodeVisitor.process(analysis.sortSymbols(), context);

        TopNProjection tnp = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                analysis.sortSymbols(),
                analysis.reverseFlags()
        );
        tnp.outputs(analysis.outputSymbols());

        plan.add(NodeBuilder.localMerge(ImmutableList.<Projection>of(tnp), collectNode));
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
    }

    private void globalAggregates(SelectAnalysis analysis, Plan plan) {
        // global aggregate: collect and partial aggregate on C and final agg on H
        Context context = new Context(Aggregation.Step.PARTIAL);
        nodeVisitor.process(analysis.outputSymbols(), context);
        nodeVisitor.process(analysis.groupBy(), context);
        nodeVisitor.process(analysis.sortSymbols(), context);

        AggregationProjection ap = new AggregationProjection();
        ap.aggregations(context.aggregationList());
        CollectNode collectNode = NodeBuilder.collect(
            analysis,
            context.symbolList(),
            ImmutableList.<Projection>of(ap)
        );

        plan.add(collectNode);

        // the hander stuff
        Context mergeContext = new Context(Aggregation.Step.FINAL);
        nodeVisitor.process(analysis.outputSymbols(), mergeContext);
        nodeVisitor.process(analysis.groupBy(), mergeContext);
        nodeVisitor.process(analysis.sortSymbols(), mergeContext);

        ap = new AggregationProjection();
        ap.aggregations(mergeContext.aggregationList());
        plan.add(NodeBuilder.localMerge(ImmutableList.<Projection>of(ap), collectNode));
    }

    private void groupBy(SelectAnalysis analysis, Plan plan) {
        if (analysis.rowGranularity().ordinal() < RowGranularity.DOC.ordinal()) {
            nonDistributedGroupBy(analysis, plan);
        } else {
            distributedGroupby(analysis, plan);
        }
    }

    private void nonDistributedGroupBy(SelectAnalysis analysis, Plan plan) {
        Context context = new Context(Aggregation.Step.FINAL, analysis.groupBy().size());
        nodeVisitor.process(analysis.outputSymbols(), context);
        nodeVisitor.process(analysis.sortSymbols(), context);
        nodeVisitor.process(analysis.groupBy(), context);

        GroupProjection groupProjection =
                new GroupProjection(analysis.groupBy(), context.aggregationList());
        CollectNode collectNode = NodeBuilder.collect(
                analysis,
                context.symbolList(),
                ImmutableList.<Projection>of(groupProjection)
        );
        plan.add(collectNode);

        // handler
        groupProjection =
                new GroupProjection(analysis.groupBy(), context.aggregationList());
        TopNProjection topN = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                analysis.sortSymbols(),
                analysis.reverseFlags()
        );
        topN.outputs(analysis.outputSymbols());

        plan.add(NodeBuilder.localMerge(ImmutableList.<Projection>of(groupProjection, topN), collectNode));
    }

    private void distributedGroupby(SelectAnalysis analysis, Plan plan) {
        // distributed collect on mapper nodes
        // merge on reducer to final (has row authority)
        // merge on handler

        Context context = new Context(Aggregation.Step.PARTIAL, analysis.groupBy().size());
        nodeVisitor.process(analysis.outputSymbols(), context);
        nodeVisitor.process(analysis.sortSymbols(), context);

        GroupProjection groupProjection =
            new GroupProjection(analysis.groupBy(), context.aggregationList());
        CollectNode collectNode = NodeBuilder.distributingCollect(
            analysis,
            context.symbolList(),
            Lists.newArrayList(analysis.table().getRouting(analysis.whereClause()).nodes()),
            ImmutableList.<Projection>of(groupProjection)
        );
        plan.add(collectNode);

        context = new Context(Aggregation.Step.FINAL, analysis.groupBy().size());
        nodeVisitor.process(analysis.outputSymbols(), context);
        nodeVisitor.process(analysis.sortSymbols(), context);
        nodeVisitor.process(analysis.groupBy(), context);

        // reducer
        List<Projection> projections = new ArrayList<>();
        projections.add(new GroupProjection(analysis.groupBy(), context.aggregationList()));
        if (analysis.limit() != null || analysis.offset() != 0) {
                TopNProjection topN = new TopNProjection(
                        Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT) + analysis.offset(),
                        0,
                        analysis.sortSymbols(),
                        analysis.reverseFlags()
                );
            topN.outputs(generateGroupByOutputs(analysis.groupBy(), context.aggregationList()));
            projections.add(topN);
        }

        MergeNode mergeNode = NodeBuilder.distributedMerge(collectNode, ImmutableList.copyOf(projections));
        plan.add(mergeNode);

        // handler
        TopNProjection topN = new TopNProjection(
                Objects.firstNonNull(analysis.limit(), Constants.DEFAULT_SELECT_LIMIT),
                analysis.offset(),
                analysis.sortSymbols(),
                analysis.reverseFlags()
        );
        topN.outputs(analysis.outputSymbols());

        plan.add(NodeBuilder.localMerge(ImmutableList.<Projection>of(topN), mergeNode));
    }

    private List<Symbol> generateGroupByOutputs(List<Symbol> groupKeys, List<Aggregation> aggregations) {
        List<Symbol> outputs = new ArrayList<>(groupKeys);
        int idx = groupKeys.size();
        for (Aggregation aggregation : aggregations) {
            outputs.add(new InputColumn(idx));
            idx++;
        }
        return outputs;
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

    private static List<DataType> extractDataTypes(List<Symbol> symbols) {
        List<DataType> types = new ArrayList<>(symbols.size());
        for (Symbol symbol : symbols) {
            types.add(symbol.accept(dataTypeVisitor, null));
        }
        return types;
    }

    private static List<DataType> extractDataTypes(List<Projection> projections, @Nullable List<DataType> inputTypes) {
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

    private static class DataTypeVisitor extends SymbolVisitor<Void, DataType> {

        @Override
        public DataType visitAggregation(Aggregation symbol, Void context) {
            if (symbol.toStep() == Aggregation.Step.PARTIAL) {
                return DataType.NULL; // TODO: change once we have aggregationState types
            }
            return symbol.functionInfo().returnType();
        }

        @Override
        public DataType visitValue(Value symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitStringLiteral(StringLiteral symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitDoubleLiteral(DoubleLiteral symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitBooleanLiteral(BooleanLiteral symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitIntegerLiteral(IntegerLiteral symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitInputColumn(InputColumn inputColumn, Void context) {
            return null;
        }

        @Override
        public DataType visitNullLiteral(Null symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitLongLiteral(LongLiteral symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitFloatLiteral(FloatLiteral symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        protected DataType visitSymbol(Symbol symbol, Void context) {
            throw new UnsupportedOperationException("Unsupported Symbol");
        }

        @Override
        public DataType visitReference(Reference symbol, Void context) {
            return symbol.valueType();
        }

        @Override
        public DataType visitFunction(Function symbol, Void context) {
            return symbol.valueType();
        }
    }
}
