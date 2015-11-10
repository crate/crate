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

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.ValidationException;
import io.crate.operation.projectors.TopN;
import io.crate.planner.TableStatsService;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.*;


@Singleton
public class CrossJoinConsumer implements Consumer {

    private final Visitor visitor;
    private final static ESLogger LOGGER = Loggers.getLogger(CrossJoinConsumer.class);

    @Inject
    public CrossJoinConsumer(ClusterService clusterService, AnalysisMetaData analysisMetaData, TableStatsService tableStatsService) {
        visitor = new Visitor(clusterService, analysisMetaData, tableStatsService);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static Map<Symbol, Field> getFieldMap(Iterable<? extends QueriedRelation> subRelations) {
        Map<Symbol, Field> fieldMap = new HashMap<>();
        for (QueriedRelation subRelation : subRelations) {
            List<Field> fields = subRelation.fields();
            QuerySpec splitQuerySpec = subRelation.querySpec();
            for (int i = 0; i < splitQuerySpec.outputs().size(); i++) {
                fieldMap.put(splitQuerySpec.outputs().get(i), fields.get(i));
            }
        }
        return fieldMap;
    }

    private static void replaceFields(QuerySpec parentQuerySpec, Map<Symbol, Symbol> symbolMap) {
        MappingSymbolVisitor.inPlace().processInplace(parentQuerySpec.outputs(), symbolMap);
        WhereClause where = parentQuerySpec.where();
        if (where != null && where.hasQuery()) {
            parentQuerySpec.where(new WhereClause(
                    MappingSymbolVisitor.inPlace().process(where.query(), symbolMap)
            ));
        }
        if (parentQuerySpec.orderBy().isPresent()) {
            MappingSymbolVisitor.inPlace().processInplace(parentQuerySpec.orderBy().get().orderBySymbols(), symbolMap);
        }
    }

    private static class Visitor extends RelationPlanningVisitor {

        private static final Predicate<MultiSourceSelect.Source> DOC_TABLE_RELATION = new Predicate<MultiSourceSelect.Source>() {
            @Override
            public boolean apply(@Nullable MultiSourceSelect.Source input) {
                if (input == null) {
                    return false;
                }
                AnalyzedRelation relation = input.relation();
                return relation instanceof DocTableRelation || relation instanceof QueriedDocTable;
            }
        };
        private final ClusterService clusterService;
        private final AnalysisMetaData analysisMetaData;
        private final TableStatsService tableStatsService;

        public Visitor(ClusterService clusterService, AnalysisMetaData analysisMetaData, TableStatsService tableStatsService) {
            this.clusterService = clusterService;
            this.analysisMetaData = analysisMetaData;
            this.tableStatsService = tableStatsService;
        }


        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect statement, ConsumerContext context) {
            boolean hasDocTables = Iterables.any(statement.sources().values(), DOC_TABLE_RELATION);
            if (isUnsupportedStatement(statement, context, hasDocTables)) return null;
            QuerySpec querySpec = statement.querySpec();
            if (querySpec.where().noMatch()) {
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }
            final Collection<QualifiedName> relationNames = getOrderedRelationNames(statement);

            List<QueriedTableRelation> queriedTables = new ArrayList<>(relationNames.size());
            List<RelationColumn> nlOutputs = new ArrayList<>();
            Map<Symbol, Symbol> symbolMap = new HashMap<>();
            for (QualifiedName relationName : relationNames) {
                MultiSourceSelect.Source source = statement.sources().get(relationName);
                QueriedTableRelation queriedTable;
                assert source.querySpec().outputs() != null;
                try {
                    queriedTable = SubRelationConverter.INSTANCE.process(source.relation(), source);
                } catch (ValidationException e) {
                    context.validationException(e);
                    return null;
                }
                int index = 0;
                for (Symbol symbol : source.querySpec().outputs()) {
                    RelationColumn rc = new RelationColumn(relationName, index++, symbol.valueType());
                    nlOutputs.add(rc);
                    symbolMap.put(symbol, rc);
                }
                queriedTables.add(queriedTable);
            }

            // for nested loops we are fine to remove pushed down orders
            OrderBy orderByBeforeSplit = querySpec.orderBy().orNull();

            // replace all the fields in the root query spec
            //Map<Symbol, Field> fieldMap = getFieldMap(queriedTables);
            replaceFields(statement.querySpec(), symbolMap);
            if (statement.remainingOrderBy().isPresent()) {
                querySpec.orderBy(statement.remainingOrderBy().get());
                MappingSymbolVisitor.inPlace().processInplace(statement.remainingOrderBy().get().orderBySymbols(), symbolMap);
            }

            WhereClause where = querySpec.where();
            boolean filterNeeded = where.hasQuery() && !(where.query() instanceof Literal);
            boolean isDistributed = hasDocTables && filterNeeded;

            QueriedTableRelation<?> left = queriedTables.get(0);
            QueriedTableRelation<?> right = queriedTables.get(1);

            if (filterNeeded || statement.remainingOrderBy().isPresent()) {
                for (QueriedTableRelation queriedTable : queriedTables) {
                    queriedTable.querySpec().limit(null);
                    queriedTable.querySpec().offset(TopN.NO_OFFSET);
                }
            }

            if (!filterNeeded && querySpec.limit().isPresent()) {
                context.requiredPageSize(querySpec.limit().get() + querySpec.offset());
            }

            // this normalization is required to replace fields of the table relations
            left.normalize(analysisMetaData);
            right.normalize(analysisMetaData);

            boolean broadcastLeftTable = false;
            if (isDistributed) {
                long leftNumDocs = tableStatsService.numDocs(left.tableRelation().tableInfo().ident());
                long rightNumDocs = tableStatsService.numDocs(right.tableRelation().tableInfo().ident());

                if (rightNumDocs > leftNumDocs) {
                    broadcastLeftTable = true;
                    LOGGER.debug("Right table is larger with {} docs (left has {}. Will change left plan to broadcast its result",
                            rightNumDocs, leftNumDocs);
                }
            }
            PlannedAnalyzedRelation leftPlan = context.plannerContext().planSubRelation(left, context);
            PlannedAnalyzedRelation rightPlan = context.plannerContext().planSubRelation(right, context);
            context.requiredPageSize(null);

            if (Iterables.any(Arrays.asList(leftPlan, rightPlan), PlannedAnalyzedRelation.IS_NOOP)) {
                // one of the plans or both are noops
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }

            Set<String> localExecutionNodes = ImmutableSet.of(clusterService.localNode().id());
            Collection<String> nlExecutionNodes = localExecutionNodes;


            MergePhase leftMerge = null;
            MergePhase rightMerge = null;
            if (isDistributed && broadcastLeftTable) {
                rightPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
                nlExecutionNodes = rightPlan.resultPhase().executionNodes();

                if (nlExecutionNodes.size() == 1
                    && nlExecutionNodes.equals(leftPlan.resultPhase().executionNodes())) {
                    // if the left and the right plan are executed on the same single node the mergePhase
                    // should be omitted. This is the case if the left and right table have only one shards which
                    // are on the same node
                    leftPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
                } else {
                    leftMerge = mergePhase(
                            context,
                            nlExecutionNodes,
                            leftPlan.resultPhase(),
                            left.querySpec().orderBy().orNull(),
                            left.querySpec().outputs(),
                            true);
                    leftPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_BROADCAST);
                }
            } else {
                if (isDistributed) {
                    leftPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
                    nlExecutionNodes = leftPlan.resultPhase().executionNodes();
                } else {
                    leftMerge = mergePhase(
                            context,
                            nlExecutionNodes,
                            leftPlan.resultPhase(),
                            left.querySpec().orderBy().orNull(),
                            left.querySpec().outputs(),
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
                            right.querySpec().orderBy().orNull(),
                            right.querySpec().outputs(),
                            isDistributed);
                    rightPlan.resultPhase().distributionInfo(DistributionInfo.DEFAULT_BROADCAST);
                }
            }

            List<Projection> projections = new ArrayList<>();

            //List<RelationColumn> inputs = new ArrayList<>(left.fields().size() + right.fields().size());
            //List<Field> inputs = concatFields(left, right);
            if (filterNeeded) {
                FilterProjection filterProjection = ProjectionBuilder.filterProjection(nlOutputs, where.query());
                projections.add(filterProjection);
            }

            List<Symbol> postNLOutputs = Lists.newArrayList(querySpec.outputs());
            if (orderByBeforeSplit != null && isDistributed) {
                for (Symbol symbol : orderByBeforeSplit.orderBySymbols()) {
                    if (postNLOutputs.indexOf(symbol) == -1) {
                        postNLOutputs.add(symbol);
                    }
                }
            }

            int topNLimit = querySpec.limit().or(Constants.DEFAULT_SELECT_LIMIT);
            TopNProjection topN = ProjectionBuilder.topNProjection(
                    nlOutputs,
                    statement.remainingOrderBy().orNull(),
                    isDistributed ? 0 : querySpec.offset(),
                    isDistributed ? topNLimit + querySpec.offset() : topNLimit,
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
            // TODO: build local merge phases somewhere else for any subplan
            if (isDistributed && context.isRoot()) {
                localMergePhase = mergePhase(context, localExecutionNodes, nl, orderByBeforeSplit, postNLOutputs, true);
                assert localMergePhase != null : "local merge phase must not be null";
                TopNProjection finalTopN = ProjectionBuilder.topNProjection(
                        postNLOutputs,
                        orderByBeforeSplit,
                        querySpec.offset(),
                        querySpec.limit().or(Constants.DEFAULT_SELECT_LIMIT),
                        querySpec.outputs()
                );
                localMergePhase.addProjection(finalTopN);
            }
            return new NestedLoop(nl, leftPlan, rightPlan, localMergePhase);
        }

        // TODO: this is a duplicate, it coecists in QueryThenFetchConsumer
        public static MergePhase mergePhase(ConsumerContext context,
                                      Collection<String> executionNodes,
                                      UpstreamPhase upstreamPhase,
                                      @Nullable OrderBy orderBy,
                                      List<Symbol> previousOutputs,
                                      boolean isDistributed) {
            assert !upstreamPhase.executionNodes().isEmpty() : "upstreamPhase must be executed somewhere";
            if (!isDistributed && upstreamPhase.executionNodes().equals(executionNodes)) {
                // if the nested loop is on the same node we don't need a mergePhase to receive requests
                // but can access the RowReceiver of the nestedLoop directly
                return null;
            }

            MergePhase mergePhase;
            if (orderBy != null) {
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
        private Collection<QualifiedName> getOrderedRelationNames(MultiSourceSelect statement) {
            if (!statement.querySpec().orderBy().isPresent()) {
                return statement.sources().keySet();
            }
            Optional<OrderBy> orderBy = statement.querySpec().orderBy();
            final List<QualifiedName> orderByOrder = new ArrayList<>(statement.sources().size());
            for (Symbol orderBySymbol : orderBy.get().orderBySymbols()) {
                for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : statement.sources().entrySet()) {
                    Optional<OrderBy> subOrderBy = entry.getValue().querySpec().orderBy();
                    if (!subOrderBy.isPresent() || orderByOrder.contains(entry.getKey())) {
                        continue;
                    }
                    if (orderBySymbol.equals(subOrderBy.get().orderBySymbols().get(0))) {
                        orderByOrder.add(entry.getKey());
                        break;
                    }
                }
            }
            if (orderByOrder.size() < statement.sources().size()) {
                Iterator<QualifiedName> iter = statement.sources().keySet().iterator();
                while (orderByOrder.size() < statement.sources().size()) {
                    QualifiedName qn = iter.next();
                    if (!orderByOrder.contains(qn)) {
                        orderByOrder.add(qn);
                    }
                }
            }
            return orderByOrder;
        }

        private boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context, boolean hasDocTables) {
            if (statement.sources().size() != 2) {
                context.validationException(new ValidationException("Joining more than 2 relations is not supported"));
                return true;
            }

            if (statement.querySpec().groupBy().isPresent()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return true;
            }

            return false;
        }
    }

    private static class SubRelationConverter extends AnalyzedRelationVisitor<MultiSourceSelect.Source, QueriedTableRelation> {

        static final SubRelationConverter INSTANCE = new SubRelationConverter();

        @Override
        public QueriedTableRelation visitTableRelation(TableRelation tableRelation,
                                                       MultiSourceSelect.Source source) {
            return new QueriedTable(tableRelation, source.querySpec());
        }

        @Override
        public QueriedTableRelation visitDocTableRelation(DocTableRelation tableRelation,
                                                          MultiSourceSelect.Source source) {
            return new QueriedDocTable(tableRelation, source.querySpec());
        }

        @Override
        protected QueriedTableRelation visitAnalyzedRelation(AnalyzedRelation relation,
                                                             MultiSourceSelect.Source source) {
            throw new ValidationException("CROSS JOIN with sub queries is not supported");
        }
    }

}
