/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.planner.consumer;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.OutputName;
import io.crate.operation.projectors.TopN;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.fetch.FetchRequiredVisitor;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;

import java.util.*;


public class CrossJoinConsumer implements Consumer {

    private final Visitor visitor;

    public CrossJoinConsumer(ClusterService clusterService,
                             AnalysisMetaData analysisMetaData) {
        visitor = new Visitor(clusterService, analysisMetaData);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final ClusterService clusterService;
        private final AnalysisMetaData analysisMetaData;
        private final SubRelationConverter subRelationConverter;

        public Visitor(ClusterService clusterService, AnalysisMetaData analysisMetaData) {
            this.clusterService = clusterService;
            this.analysisMetaData = analysisMetaData;
            subRelationConverter = new SubRelationConverter(analysisMetaData);
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect statement, ConsumerContext context) {
            if (isUnsupportedStatement(statement, context)) return null;
            if (statement.querySpec().where().noMatch()) {
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }

            final Map<Object, Integer> relationOrder = getRelationOrder(statement);

            // TODO: replace references with docIds.. and add fetch projection

            List<QueriedTableRelation> queriedTables = new ArrayList<>();
            for (Map.Entry<QualifiedName, AnalyzedRelation> entry : statement.sources().entrySet()) {
                AnalyzedRelation analyzedRelation = entry.getValue();
                QueriedTableRelation queriedTable;
                try {
                    queriedTable = subRelationConverter.process(analyzedRelation, statement);
                } catch (ValidationException e) {
                    context.validationException(e);
                    return null;
                }
                queriedTables.add(queriedTable);
            }

            WhereClause where = statement.querySpec().where();
            OrderBy orderBy = statement.querySpec().orderBy();
            if (where.hasQuery() && !(where.query() instanceof Literal)) {
                throw new UnsupportedOperationException("JOIN condition in the WHERE clause is not supported");
            }

            boolean hasRemainingOrderBy = orderBy != null && orderBy.isSorted();
            if (hasRemainingOrderBy) {
                for (QueriedTableRelation queriedTable : queriedTables) {
                    queriedTable.querySpec().limit(null);
                    queriedTable.querySpec().offset(TopN.NO_OFFSET);
                }
            }
            sortQueriedTables(relationOrder, queriedTables);


            QueriedTableRelation<?> left = queriedTables.get(0);
            QueriedTableRelation<?> right = queriedTables.get(1);

            Integer limit = statement.querySpec().limit();
            if (limit != null) {
                context.requiredPageSize(limit + statement.querySpec().offset());
            }
            PlannedAnalyzedRelation leftPlan = context.plannerContext().planSubRelation(left, context);
            PlannedAnalyzedRelation rightPlan = context.plannerContext().planSubRelation(right, context);
            context.requiredPageSize(null);

            Set<String> localExecutionNodes = ImmutableSet.of(clusterService.localNode().id());

            MergePhase leftMerge = mergePhase(
                    context,
                    localExecutionNodes,
                    leftPlan.resultPhase(),
                    left.querySpec());
            MergePhase rightMerge = mergePhase(
                    context,
                    localExecutionNodes,
                    rightPlan.resultPhase(),
                    right.querySpec());


            ProjectionBuilder projectionBuilder = new ProjectionBuilder(analysisMetaData.functions(), statement.querySpec());

            List<Field> inputs = new ArrayList<>(
                    left.querySpec().outputs().size() + right.querySpec().outputs().size());


            inputs.addAll(left.fields());
            inputs.addAll(right.fields());

            TopNProjection topN = projectionBuilder.topNProjection(
                    inputs,
                    statement.querySpec().orderBy(),
                    statement.querySpec().offset(),
                    statement.querySpec().limit(),
                    statement.querySpec().outputs()
            );

            NestedLoopPhase nl = new NestedLoopPhase(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    "nested-loop",
                    ImmutableList.<Projection>of(topN),
                    leftMerge,
                    rightMerge,
                    localExecutionNodes
            );

            return new NestedLoop(nl, leftPlan, rightPlan, true);
        }

        @Nullable
        private MergePhase mergePhase(ConsumerContext context,
                                      Set<String> localExecutionNodes,
                                      UpstreamPhase upstreamPhase,
                                      QuerySpec querySpec) {
            return mergePhase(context, localExecutionNodes, upstreamPhase, querySpec.outputs(), querySpec.orderBy());
        }

        @Nullable
        private MergePhase mergePhase(ConsumerContext context,
                                      Set<String> localExecutionNodes,
                                      UpstreamPhase upstreamPhase,
                                      List<Symbol> previousOutputs,
                                      @Nullable OrderBy orderBy) {
            if (upstreamPhase.executionNodes().isEmpty()
                || upstreamPhase.executionNodes().equals(localExecutionNodes)) {
                // if the nested loop is on the same node we don't need a mergePhase to receive requests
                // but can access the RowReceiver of the nestedLoop directly
                return null;
            }

            if (upstreamPhase instanceof MergePhase && upstreamPhase.executionNodes().isEmpty()) {
                ((MergePhase) upstreamPhase).executionNodes(localExecutionNodes);
            }
            MergePhase mergePhase;
            if (OrderBy.isSorted(orderBy)) {
                mergePhase = MergePhase.sortedMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        orderBy,
                        previousOutputs,
                        orderBy.orderBySymbols(),
                        ImmutableList.<Projection>of(),
                        upstreamPhase.executionNodes().size(),
                        Symbols.extractTypes(previousOutputs)
                );
            } else {
                mergePhase = MergePhase.localMerge(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        ImmutableList.<Projection>of(),
                        upstreamPhase.executionNodes().size(),
                        Symbols.extractTypes(previousOutputs)
                );
            }
            mergePhase.executionNodes(localExecutionNodes);
            return mergePhase;
        }

        private void sortQueriedTables(final Map<Object, Integer> relationOrder, List<QueriedTableRelation> queriedTables) {
            Collections.sort(queriedTables, new Comparator<QueriedTableRelation>() {
                @Override
                public int compare(QueriedTableRelation o1, QueriedTableRelation o2) {
                    return Integer.compare(
                            MoreObjects.firstNonNull(relationOrder.get(o1.tableRelation()), Integer.MAX_VALUE),
                            MoreObjects.firstNonNull(relationOrder.get(o2.tableRelation()), Integer.MAX_VALUE));
                }
            });
        }

        /**
         * returns a map with the relation as keys and the values are their order in occurrence in the order by clause.
         * <p/>
         * e.g. select * from t1, t2, t3 order by t2.x, t3.y
         * <p/>
         * returns: {
         * t2: 0                 (first)
         * t3: 1                 (second)
         * }
         */
        private Map<Object, Integer> getRelationOrder(MultiSourceSelect statement) {
            OrderBy orderBy = statement.querySpec().orderBy();
            if (orderBy == null || !orderBy.isSorted()) {
                return Collections.emptyMap();
            }

            final Map<Object, Integer> orderByOrder = new IdentityHashMap<>();
            int idx = 0;
            for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
                for (AnalyzedRelation analyzedRelation : statement.sources().values()) {
                    QuerySplitter.RelationCount relationCount = QuerySplitter.getRelationCount(analyzedRelation, orderBySymbol);
                    if (relationCount != null && relationCount.numOther == 0 && relationCount.numThis > 0 &&
                        !orderByOrder.containsKey(analyzedRelation)) {
                        orderByOrder.put(analyzedRelation, idx);
                    }
                }
                idx++;
            }
            return orderByOrder;
        }

        private boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.sources().size() != 2) {
                context.validationException(new ValidationException("Joining more than 2 relations is not supported"));
                return true;
            }

            List<Symbol> groupBy = statement.querySpec().groupBy();
            if (groupBy != null && !groupBy.isEmpty()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return true;
            }

            if (hasOutputsToFetch(statement.querySpec())) {
                context.validationException(new ValidationException("Only fields that are used in ORDER BY can be selected within a CROSS JOIN"));
                return true;
            }
            return false;
        }

        private boolean hasOutputsToFetch(QuerySpec querySpec) {
            FetchRequiredVisitor.Context ctx = new FetchRequiredVisitor.Context(querySpec.orderBy());
            return FetchRequiredVisitor.INSTANCE.process(querySpec.outputs(), ctx);
        }


    }

    private static class SubRelationConverter extends AnalyzedRelationVisitor<MultiSourceSelect, QueriedTableRelation> {

        private static final QueriedTableFactory<QueriedTable, TableRelation> QUERIED_TABLE_FACTORY =
                new QueriedTableFactory<QueriedTable, TableRelation>() {
                    @Override
                    public QueriedTable create(TableRelation tableRelation, List<OutputName> outputNames, QuerySpec querySpec) {
                        return new QueriedTable(tableRelation, outputNames, querySpec);
                    }
                };
        private static final QueriedTableFactory<QueriedDocTable, DocTableRelation> QUERIED_DOC_TABLE_FACTORY =
                new QueriedTableFactory<QueriedDocTable, DocTableRelation>() {
                    @Override
                    public QueriedDocTable create(DocTableRelation tableRelation, List<OutputName> outputNames, QuerySpec querySpec) {
                        return new QueriedDocTable(tableRelation, outputNames, querySpec);
                    }
                };

        private final AnalysisMetaData analysisMetaData;

        public SubRelationConverter(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        public QueriedTableRelation visitTableRelation(TableRelation tableRelation,
                                                       MultiSourceSelect statement) {
            return newSubRelation(tableRelation, statement.querySpec(), QUERIED_TABLE_FACTORY);
        }

        @Override
        public QueriedTableRelation visitDocTableRelation(DocTableRelation tableRelation,
                                                          MultiSourceSelect statement) {
            return newSubRelation(tableRelation, statement.querySpec(), QUERIED_DOC_TABLE_FACTORY);
        }

        @Override
        protected QueriedTableRelation visitAnalyzedRelation(AnalyzedRelation relation,
                                                             MultiSourceSelect statement) {
            throw new ValidationException("CROSS JOIN with sub queries is not supported");
        }


        /**
         * Create a new concrete QueriedTableRelation implementation for the given
         * AbstractTableRelation implementation
         * <p/>
         * It will walk through all symbols from QuerySpec and pull-down any symbols that the
         * new QueriedTable can handle. The symbols that are pulled down from the original
         * querySpec will be replaced with symbols that point to a output/field of the
         * QueriedTableRelation.
         */
        private <QT extends QueriedTableRelation, TR extends AbstractTableRelation> QT newSubRelation(TR tableRelation,
                                                                                                      QuerySpec querySpec,
                                                                                                      QueriedTableFactory<QT, TR> factory) {
            RelationSplitter.SplitQuerySpecContext context = RelationSplitter.splitQuerySpec(tableRelation, querySpec);
            QT queriedTable = factory.create(tableRelation, context.outputNames(), context.querySpec());
            RelationSplitter.replaceFields(queriedTable, querySpec, context.querySpec());
            queriedTable.normalize(analysisMetaData);
            return queriedTable;
        }

        private interface QueriedTableFactory<QT extends QueriedTableRelation, TR extends AbstractTableRelation> {
            QT create(TR tableRelation, List<OutputName> outputNames, QuerySpec querySpec);
        }

    }

}
