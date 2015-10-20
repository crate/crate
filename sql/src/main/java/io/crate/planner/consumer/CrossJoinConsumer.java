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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.OutputName;
import io.crate.operation.projectors.TopN;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.fetch.FetchRequiredVisitor;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;

import java.util.*;

import static com.google.common.base.MoreObjects.firstNonNull;


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

            SubRelationConverterContext subRelationConverterContext = new SubRelationConverterContext(statement.querySpec());
            List<QueriedTableRelation> queriedTables = new ArrayList<>();
            for (Map.Entry<QualifiedName, AnalyzedRelation> entry : statement.sources().entrySet()) {
                AnalyzedRelation analyzedRelation = entry.getValue();
                QueriedTableRelation queriedTable;
                try {
                    queriedTable = subRelationConverter.process(analyzedRelation, subRelationConverterContext);
                } catch (ValidationException e) {
                    context.validationException(e);
                    return null;
                }
                queriedTables.add(queriedTable);
            }

            WhereClause where = statement.querySpec().where();
            OrderBy orderBy = subRelationConverterContext.remainingOrderBy();
            OrderBy orderByBeforeSplit = statement.querySpec().orderBy();

            boolean hasRemainingOrderBy = orderBy != null && orderBy.isSorted();
            boolean isFilterNeeded = where.hasQuery() && !(where.query() instanceof Literal);
            if (hasRemainingOrderBy || isFilterNeeded) {
                for (QueriedTableRelation queriedTable : queriedTables) {
                    queriedTable.querySpec().limit(null);
                    queriedTable.querySpec().offset(TopN.NO_OFFSET);
                }
            }
            sortQueriedTables(relationOrder, queriedTables);

            // TODO: do we always distribute if filter is needed?
            boolean isDistributed = isFilterNeeded;

            QueriedTableRelation<?> left = queriedTables.get(0);
            QueriedTableRelation<?> right = queriedTables.get(1);

            Integer limit = statement.querySpec().limit();
            if (!isFilterNeeded && limit != null) {
                context.requiredPageSize(limit + statement.querySpec().offset());
            }
            PlannedAnalyzedRelation leftPlan = context.plannerContext().planSubRelation(left, context);
            PlannedAnalyzedRelation rightPlan = context.plannerContext().planSubRelation(right, context);
            context.requiredPageSize(null);

            Set<String> localExecutionNodes = ImmutableSet.of(clusterService.localNode().id());
            Collection<String> nlExecutionNodes = localExecutionNodes;

            MergePhase leftMerge = null;
            MergePhase rightMerge = null;
            if (isDistributed) {
                leftPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
                nlExecutionNodes = leftPlan.resultPhase().executionNodes();
            } else {
                leftMerge = mergePhase(
                        context,
                        nlExecutionNodes,
                        leftPlan.resultPhase(),
                        left.querySpec(),
                        false);
            }
            if (nlExecutionNodes.size() == 1
                    && nlExecutionNodes.equals(rightPlan.resultPhase().executionNodes())) {
                // if the left and the right plan are executed on the same single node the mergePhase
                // should be omitted. This is the case if the left and right table have only one shards which
                // are on the same node
                rightPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            } else {
                rightMerge = mergePhase(
                        context,
                        nlExecutionNodes,
                        rightPlan.resultPhase(),
                        right.querySpec(),
                        isDistributed);
                rightPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }

            ProjectionBuilder projectionBuilder = new ProjectionBuilder(analysisMetaData.functions(), statement.querySpec());

            List<Field> inputs = new ArrayList<>(
                    left.querySpec().outputs().size() + right.querySpec().outputs().size());

            inputs.addAll(left.fields());
            inputs.addAll(right.fields());

            List<Projection> projections = new ArrayList<>();

            if (isFilterNeeded) {
                FilterProjection filterProjection = projectionBuilder.filterProjection(inputs, where.query());
                projections.add(filterProjection);
            }

            List<Symbol> postNLOutputs = Lists.newArrayList(statement.querySpec().outputs());
            if (orderByBeforeSplit != null && isDistributed) {
                for (Symbol symbol : orderByBeforeSplit.orderBySymbols()) {
                    if (postNLOutputs.indexOf(symbol) == -1) {
                        postNLOutputs.add(symbol);
                    }
                }
            }


            int topNLimit = firstNonNull(statement.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topN = projectionBuilder.topNProjection(
                    inputs,
                    hasRemainingOrderBy ? orderByBeforeSplit : orderBy,
                    isDistributed ? 0 : statement.querySpec().offset(),
                    isDistributed ? topNLimit + statement.querySpec().offset() : topNLimit,
                    postNLOutputs
            );
            projections.add(topN);

            NestedLoopPhase nl = new NestedLoopPhase(
                    context.plannerContext().jobId(),
                    context.plannerContext().nextExecutionPhaseId(),
                    isDistributed ? "distributed-nested-loop" : "nested-loop",
                    projections,
                    leftMerge,
                    rightMerge,
                    nlExecutionNodes
            );
            if (isDistributed) {
                nl.distributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }

            MergePhase localMergePhase = null;
            if (isDistributed && context.rootRelation() == statement) {
                localMergePhase = mergePhase(context, localExecutionNodes, nl, postNLOutputs,
                        orderByBeforeSplit, true);
                assert localMergePhase != null : "local merge phase must not be null";
                TopNProjection finalTopN = projectionBuilder.topNProjection(
                        postNLOutputs,
                        orderByBeforeSplit,
                        statement.querySpec().offset(),
                        statement.querySpec().limit(),
                        statement.querySpec().outputs()
                );
                localMergePhase.addProjection(finalTopN);
            }

            return new NestedLoop(nl, leftPlan, rightPlan, true, localMergePhase);
        }

        @Nullable
        private MergePhase mergePhase(ConsumerContext context,
                                      Collection<String> executionNodes,
                                      UpstreamPhase upstreamPhase,
                                      QuerySpec querySpec,
                                      boolean isDistributed) {
            return mergePhase(context, executionNodes, upstreamPhase, querySpec.outputs(),
                    querySpec.orderBy(), isDistributed);
        }

        @Nullable
        private MergePhase mergePhase(ConsumerContext context,
                                      Collection<String> executionNodes,
                                      UpstreamPhase upstreamPhase,
                                      List<Symbol> previousOutputs,
                                      @Nullable OrderBy orderBy,
                                      boolean isDistributed) {
            if (!isDistributed
                    && (upstreamPhase.executionNodes().isEmpty()
                    || upstreamPhase.executionNodes().equals(executionNodes))) {
                // if the nested loop is on the same node we don't need a mergePhase to receive requests
                // but can access the RowReceiver of the nestedLoop directly
                return null;
            }

            if (upstreamPhase instanceof MergePhase && upstreamPhase.executionNodes().isEmpty()) {
                ((MergePhase) upstreamPhase).executionNodes(executionNodes);
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
            mergePhase.executionNodes(executionNodes);
            return mergePhase;
        }

        private void sortQueriedTables(final Map<Object, Integer> relationOrder, List<QueriedTableRelation> queriedTables) {
            Collections.sort(queriedTables, new Comparator<QueriedTableRelation>() {
                @Override
                public int compare(QueriedTableRelation o1, QueriedTableRelation o2) {
                    return Integer.compare(
                            firstNonNull(relationOrder.get(o1.tableRelation()), Integer.MAX_VALUE),
                            firstNonNull(relationOrder.get(o2.tableRelation()), Integer.MAX_VALUE));
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

    private static class SubRelationConverterContext {

        private final QuerySpec querySpec;
        private OrderBy remainingOrderBy;

        public SubRelationConverterContext(QuerySpec querySpec) {
            this.querySpec = querySpec;
            remainingOrderBy = querySpec.orderBy();
        }

        public QuerySpec querySpec() {
            return querySpec;
        }

        public void remainingOrderBy(OrderBy remainingOrderBy) {
            this.remainingOrderBy = remainingOrderBy;
        }

        public OrderBy remainingOrderBy() {
            return remainingOrderBy;
        }
    }

    private static class SubRelationConverter extends AnalyzedRelationVisitor<SubRelationConverterContext, QueriedTableRelation> {

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
                                                       SubRelationConverterContext context) {
            return newSubRelation(tableRelation, context, QUERIED_TABLE_FACTORY);
        }

        @Override
        public QueriedTableRelation visitDocTableRelation(DocTableRelation tableRelation,
                                                          SubRelationConverterContext context) {
            return newSubRelation(tableRelation, context, QUERIED_DOC_TABLE_FACTORY);
        }

        @Override
        protected QueriedTableRelation visitAnalyzedRelation(AnalyzedRelation relation,
                                                             SubRelationConverterContext context) {
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
                                                                                                      SubRelationConverterContext context,
                                                                                                      QueriedTableFactory<QT, TR> factory) {
            RelationSplitter.SplitQuerySpecContext splitContext = RelationSplitter.splitQuerySpec(
                    tableRelation, context.querySpec(), context.remainingOrderBy());
            context.remainingOrderBy(splitContext.remainingOrderBy());
            QT queriedTable = factory.create(tableRelation, splitContext.outputNames(), splitContext.querySpec());
            RelationSplitter.replaceFields(queriedTable, context.querySpec(), splitContext.querySpec());

            queriedTable.normalize(analysisMetaData);
            return queriedTable;
        }

        private interface QueriedTableFactory<QT extends QueriedTableRelation, TR extends AbstractTableRelation> {
            QT create(TR tableRelation, List<OutputName> outputNames, QuerySpec querySpec);
        }
    }
}
