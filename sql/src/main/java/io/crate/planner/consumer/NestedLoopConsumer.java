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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.TableIdent;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Planner;
import io.crate.planner.TableStatsService;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.distribution.UpstreamPhase;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.projection.builder.InputCreatingVisitor;
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
public class NestedLoopConsumer implements Consumer {

    private final Visitor visitor;
    private final static ESLogger LOGGER = Loggers.getLogger(NestedLoopConsumer.class);

    @Inject
    public NestedLoopConsumer(ClusterService clusterService, AnalysisMetaData analysisMetaData, TableStatsService tableStatsService) {
        visitor = new Visitor(clusterService, analysisMetaData, tableStatsService);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
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

        private final ClusterService clusterService;
        private final AnalysisMetaData analysisMetaData;
        private final TableStatsService tableStatsService;

        public Visitor(ClusterService clusterService, AnalysisMetaData analysisMetaData, TableStatsService tableStatsService) {
            this.clusterService = clusterService;
            this.analysisMetaData = analysisMetaData;
            this.tableStatsService = tableStatsService;
        }

        @Override
        public PlannedAnalyzedRelation visitTwoTableJoin(TwoTableJoin statement, ConsumerContext context) {
            QuerySpec querySpec = statement.querySpec();
            if (querySpec.where().noMatch()) {
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }

            List<RelationColumn> nlOutputs = new ArrayList<>();
            final Map<Symbol, Symbol> symbolMap = new HashMap<>();
            QueriedRelation left;
            QueriedRelation right;
            try {
                left = SubRelationConverter.INSTANCE.process(statement.left().relation(), statement.left());
                right = SubRelationConverter.INSTANCE.process(statement.right().relation(), statement.right());
                addOutputsAndSymbolMap(statement.left().querySpec().outputs(), statement.leftName(), nlOutputs, symbolMap);
                addOutputsAndSymbolMap(statement.right().querySpec().outputs(), statement.rightName(), nlOutputs, symbolMap);
            } catch (ValidationException e) {
                context.validationException(e);
                return null;
            }

            // for nested loops we are fine to remove pushed down orders
            OrderBy orderByBeforeSplit = querySpec.orderBy().orNull();

            // replace all the fields in the root query spec
            replaceFields(statement.querySpec(), symbolMap);
            if (statement.remainingOrderBy().isPresent()) {
                querySpec.orderBy(statement.remainingOrderBy().get());
                MappingSymbolVisitor.inPlace().processInplace(statement.remainingOrderBy().get().orderBySymbols(), symbolMap);
            }

            JoinPair joinPair = statement.joinPair();
            JoinType joinType = joinPair.joinType();
            Symbol joinCondition = joinPair.condition();
            if (joinCondition != null) {
                // replace all fields of the join condition
                MappingSymbolVisitor.inPlace().process(joinCondition, symbolMap);
            }

            WhereClause where = querySpec.where();
            boolean filterNeeded = where.hasQuery() && !(where.query() instanceof Literal);
            boolean hasDocTables = left instanceof QueriedDocTable || right instanceof QueriedDocTable;
            boolean isDistributed = hasDocTables && filterNeeded && !joinType.isOuter();

            if (filterNeeded || joinCondition != null || statement.remainingOrderBy().isPresent()) {
                left.querySpec().limit(null);
                right.querySpec().limit(null);
                left.querySpec().offset(TopN.NO_OFFSET);
                right.querySpec().offset(TopN.NO_OFFSET);
            }

            if (!filterNeeded && joinCondition == null && querySpec.limit().isPresent()) {
                context.requiredPageSize(querySpec.limit().get() + querySpec.offset());
            }

            // this normalization is required to replace fields of the table relations
            if (left instanceof QueriedTableRelation) {
                ((QueriedTableRelation) left).normalize(analysisMetaData, context.plannerContext().statementContext());
            }
            if (right instanceof QueriedTableRelation) {
                ((QueriedTableRelation) right).normalize(analysisMetaData, context.plannerContext().statementContext());
            }

            PlannedAnalyzedRelation leftPlan = context.plannerContext().planSubRelation(left, context);
            PlannedAnalyzedRelation rightPlan = context.plannerContext().planSubRelation(right, context);
            context.requiredPageSize(null);

            if (Iterables.any(Arrays.asList(leftPlan, rightPlan), PlannedAnalyzedRelation.IS_NOOP)) {
                // one of the plans or both are noops
                return new NoopPlannedAnalyzedRelation(statement, context.plannerContext().jobId());
            }

            boolean broadcastLeftTable = false;
            if (isDistributed) {
                broadcastLeftTable = isLeftSmallerThanRight(left, right);
                if (broadcastLeftTable) {
                    PlannedAnalyzedRelation tmpPlan = leftPlan;
                    leftPlan = rightPlan;
                    rightPlan = tmpPlan;

                    QueriedRelation tmpRelation = left;
                    left = right;
                    right = tmpRelation;
                    joinType = joinType.invert();
                }
            }
            Set<String> handlerNodes = ImmutableSet.of(clusterService.localNode().id());
            Collection<String> nlExecutionNodes = handlerNodes;

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


            if (broadcastLeftTable) {
                PlannedAnalyzedRelation tmpPlan = leftPlan;
                leftPlan = rightPlan;
                rightPlan = tmpPlan;
                leftMerge = rightMerge;
                rightMerge = null;
            }
            List<Projection> projections = new ArrayList<>();

            Symbol filterSymbol = null;
            if (filterNeeded) {
                InputCreatingVisitor.Context inputVisitorContext = new InputCreatingVisitor.Context(nlOutputs);
                filterSymbol = InputCreatingVisitor.INSTANCE.process(where.query(), inputVisitorContext);
                assert filterSymbol instanceof Function : "Only function symbols are allowed for filtering";
            }
            if (joinCondition != null) {
                InputCreatingVisitor.Context inputVisitorContext = new InputCreatingVisitor.Context(nlOutputs);
                joinCondition = InputCreatingVisitor.INSTANCE.process(joinCondition, inputVisitorContext);
                assert joinCondition instanceof Function : "Only function symbols are valid join conditions";
            }

            List<Symbol> postNLOutputs = Lists.newArrayList(querySpec.outputs());
            if (orderByBeforeSplit != null && isDistributed) {
                for (Symbol symbol : orderByBeforeSplit.orderBySymbols()) {
                    if (postNLOutputs.indexOf(symbol) == -1) {
                        postNLOutputs.add(symbol);
                    }
                }
            }

            Planner.Context.Limits limits = context.plannerContext().getLimits(context.isRoot(), querySpec);
            OrderBy orderBy = statement.remainingOrderBy().orNull();
            if (orderBy == null && joinType.isOuter()) {
                orderBy = orderByBeforeSplit;
            }
            TopNProjection topN = ProjectionBuilder.topNProjection(
                nlOutputs,
                orderBy,
                isDistributed ? 0 : querySpec.offset(),
                isDistributed ? limits.limitAndOffset() : limits.finalLimit(),
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
                nlExecutionNodes,
                joinType,
                joinCondition,
                filterSymbol,
                left.querySpec().outputs().size(),
                right.querySpec().outputs().size()
            );
            MergePhase localMergePhase = null;
            // TODO: build local merge phases somewhere else for any subplan
            if (isDistributed && context.isRoot()) {
                localMergePhase = mergePhase(context, handlerNodes, nl, orderByBeforeSplit, postNLOutputs, true);
                assert localMergePhase != null : "local merge phase must not be null";
                TopNProjection finalTopN = ProjectionBuilder.topNProjection(
                        postNLOutputs,
                        null, // orderBy = null because mergePhase receives data sorted
                        querySpec.offset(),
                        limits.finalLimit(),
                        querySpec.outputs()
                );
                localMergePhase.addProjection(finalTopN);
            }
            return new NestedLoop(nl, leftPlan, rightPlan, localMergePhase, handlerNodes);
        }

        private void addOutputsAndSymbolMap(Iterable<? extends Symbol> outputs,
                                            QualifiedName name,
                                            List<RelationColumn> nlOutputs,
                                            Map<Symbol, Symbol> symbolMap) {
            int index = 0;
            for (Symbol symbol : outputs) {
                RelationColumn rc = new RelationColumn(name, index++, symbol.valueType());
                nlOutputs.add(rc);
                symbolMap.put(symbol, rc);
            }
        }

        private boolean isLeftSmallerThanRight(QueriedRelation qrLeft, QueriedRelation qrRight) {
            if (qrLeft instanceof QueriedTableRelation && qrRight instanceof QueriedTableRelation) {
                return isLeftSmallerThanRight(
                        ((QueriedTableRelation) qrLeft).tableRelation().tableInfo().ident(),
                        ((QueriedTableRelation) qrRight).tableRelation().tableInfo().ident()
                );
            }
            return false;
        }

        private boolean isLeftSmallerThanRight(TableIdent leftIdent, TableIdent rightIdent) {
            long leftNumDocs = tableStatsService.numDocs(leftIdent);
            long rightNumDocs = tableStatsService.numDocs(rightIdent);

            if (leftNumDocs < rightNumDocs) {
                LOGGER.debug("Right table is larger with {} docs (left has {}. Will change left plan to broadcast its result",
                        rightNumDocs, leftNumDocs);
                return true;
            }
            return false;
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
    }

    private static class SubRelationConverter extends AnalyzedRelationVisitor<MultiSourceSelect.Source, QueriedRelation> {

        static final SubRelationConverter INSTANCE = new SubRelationConverter();

        @Override
        public QueriedRelation visitTableRelation(TableRelation tableRelation,
                                                       MultiSourceSelect.Source source) {
            return new QueriedTable(tableRelation, source.querySpec());
        }

        @Override
        public QueriedRelation visitDocTableRelation(DocTableRelation tableRelation,
                                                          MultiSourceSelect.Source source) {
            return new QueriedDocTable(tableRelation, source.querySpec());
        }

        @Override
        public QueriedRelation visitTwoTableJoin(TwoTableJoin twoTableJoin, MultiSourceSelect.Source context) {
            return twoTableJoin;
        }

        @Override
        public QueriedRelation visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, MultiSourceSelect.Source context) {
            return new QueriedTable(tableFunctionRelation, context.querySpec());
        }

        @Override
        protected QueriedTableRelation visitAnalyzedRelation(AnalyzedRelation relation,
                                                             MultiSourceSelect.Source source) {
            throw new ValidationException("JOIN with sub queries is not supported");
        }
    }

}
