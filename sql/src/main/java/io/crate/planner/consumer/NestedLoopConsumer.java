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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.TwoTableJoin;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitors;
import io.crate.analyze.symbol.Symbols;
import io.crate.collections.Lists2;
import io.crate.metadata.TableIdent;
import io.crate.operation.projectors.TopN;
import io.crate.planner.Limits;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.PositionalOrderBy;
import io.crate.planner.ResultDescription;
import io.crate.planner.TableStats;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.InputColumns;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

class NestedLoopConsumer implements Consumer {

    private final static Logger LOGGER = Loggers.getLogger(NestedLoopConsumer.class);
    private final Visitor visitor;

    NestedLoopConsumer(ClusterService clusterService, TableStats tableStats) {
        visitor = new Visitor(clusterService, tableStats);
    }

    @Override
    public Plan consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ClusterService clusterService;
        private final TableStats tableStats;

        public Visitor(ClusterService clusterService, TableStats tableStats) {
            this.clusterService = clusterService;
            this.tableStats = tableStats;
        }

        @Override
        public Plan visitTwoTableJoin(TwoTableJoin statement, ConsumerContext context) {
            QuerySpec querySpec = statement.querySpec();

            QueriedRelation left = statement.left();
            QueriedRelation right = statement.right();
            List<Symbol> nlOutputs = Lists2.concat(left.fields(), right.fields());

            // for nested loops we are fine to remove pushed down orders
            OrderBy orderByBeforeSplit = querySpec.orderBy().orElse(null);

            if (statement.remainingOrderBy().isPresent()) {
                querySpec.orderBy(statement.remainingOrderBy().get());
            }

            JoinPair joinPair = statement.joinPair();
            JoinType joinType = joinPair.joinType();
            Symbol joinCondition = joinPair.condition();

            WhereClause where = querySpec.where();
            /*
             * isDistributed/filterNeeded doesn't consider the joinCondition.
             * This means joins with implicit syntax result in a different plan than joins using explicit syntax.
             * This was unintentional but we'll keep it that way (for now) as a distributed plan can be significantly slower
             * (depending on the number of rows that are filtered)
             * and we don't want to introduce a performance regression.
             *
             * We may at some point add some kind of session-settings to override this behaviour or otherwise
             * come up with a better heuristic.
             */
            boolean filterNeeded = where.hasQuery() && !(where.query() instanceof Literal);
            boolean hasDocTables = left instanceof QueriedDocTable || right instanceof QueriedDocTable;
            boolean isDistributed = hasDocTables && filterNeeded && !joinType.isOuter();
            Limits limits = context.plannerContext().getLimits(querySpec);

            if (!filterNeeded && joinCondition == null && querySpec.limit().isPresent()) {
                context.requiredPageSize(limits.limitAndOffset());
            }

            Plan leftPlan = context.plannerContext().planSubRelation(left, context);
            Plan rightPlan = context.plannerContext().planSubRelation(right, context);
            context.requiredPageSize(null);

            ResultDescription leftResultDesc = leftPlan.resultDescription();
            ResultDescription rightResultDesc = rightPlan.resultDescription();
            isDistributed = isDistributed &&
                            (!leftResultDesc.nodeIds().isEmpty() && !rightResultDesc.nodeIds().isEmpty());
            boolean broadcastLeftTable = false;
            if (isDistributed) {
                broadcastLeftTable = joinType != JoinType.SEMI && isLeftSmallerThanRight(left, right);
                if (broadcastLeftTable) {
                    Plan tmpPlan = leftPlan;
                    leftPlan = rightPlan;
                    rightPlan = tmpPlan;

                    QueriedRelation tmpRelation = left;
                    left = right;
                    right = tmpRelation;
                    joinType = joinType.invert();
                    leftResultDesc = leftPlan.resultDescription();
                    rightResultDesc = rightPlan.resultDescription();
                }
            }
            Collection<String> nlExecutionNodes = ImmutableSet.of(clusterService.localNode().getId());

            MergePhase leftMerge = null;
            MergePhase rightMerge = null;
            if (isDistributed && subPlanHashNoLimits(leftResultDesc)) {
                leftPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
                nlExecutionNodes = leftResultDesc.nodeIds();
            } else {
                leftPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
                if (isMergePhaseNeeded(nlExecutionNodes, leftResultDesc, false)) {
                    leftMerge = buildMergePhase(context.plannerContext(), left, leftResultDesc, nlExecutionNodes);
                }
            }
            if (nlExecutionNodes.size() == 1
                && nlExecutionNodes.equals(rightResultDesc.nodeIds())
                && subPlanHashNoLimits(rightResultDesc)) {
                // if the left and the right plan are executed on the same single node the mergePhase
                // should be omitted. This is the case if the left and right table have only one shards which
                // are on the same node
                rightPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            } else {
                if (isMergePhaseNeeded(nlExecutionNodes, rightResultDesc, isDistributed)) {
                    rightMerge = buildMergePhase(context.plannerContext(), right, rightResultDesc, nlExecutionNodes);
                }
                rightPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }

            if (broadcastLeftTable) {
                Plan tmpPlan = leftPlan;
                leftPlan = rightPlan;
                rightPlan = tmpPlan;
                leftMerge = rightMerge;
                rightMerge = null;
            }
            List<Projection> projections = new ArrayList<>();

            if (filterNeeded) {
                projections.add(ProjectionBuilder.filterProjection(nlOutputs, where));
            }
            if (joinCondition != null) {
                joinCondition = InputColumns.create(joinCondition, nlOutputs);
                assert joinCondition instanceof Function : "Only function symbols are valid join conditions";
                assert !SymbolVisitors.any(Symbols.IS_COLUMN, joinCondition)
                    : "Processed joinCondition must not contain column symbols.\njoinCondition="
                      + joinCondition + " nlOutputs=" + nlOutputs;
            }

            List<Symbol> postNLOutputs = Lists.newArrayList(querySpec.outputs());
            if (orderByBeforeSplit != null && isDistributed) {
                for (Symbol symbol : orderByBeforeSplit.orderBySymbols()) {
                    if (postNLOutputs.indexOf(symbol) == -1) {
                        postNLOutputs.add(symbol);
                    }
                }
            }

            OrderBy orderBy = statement.remainingOrderBy().orElse(null);
            if (orderBy == null && joinType.isOuter()) {
                orderBy = orderByBeforeSplit;
            }

            int limit = isDistributed ? limits.limitAndOffset() : limits.finalLimit();
            Projection topN = ProjectionBuilder.topNOrEval(
                nlOutputs,
                orderBy,
                isDistributed ? 0 : limits.offset(),
                limit,
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
                left.querySpec().outputs().size(),
                right.querySpec().outputs().size()
            );

            // postNLOutputs includes orderBy only symbols, these need to be stripped in the handlerMerge
            int postMergeNumOutput = querySpec.outputs().size();
            if (isDistributed) {
                return new NestedLoop(
                    nl,
                    leftPlan,
                    rightPlan,
                    limits.finalLimit(),
                    limits.offset(),
                    limit,
                    postMergeNumOutput,
                    PositionalOrderBy.of(orderByBeforeSplit, postNLOutputs)
                );
            } else {
                return new NestedLoop(nl, leftPlan, rightPlan, TopN.NO_LIMIT, 0, limit, postMergeNumOutput, null);
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
            long leftNumDocs = tableStats.numDocs(leftIdent);
            long rightNumDocs = tableStats.numDocs(rightIdent);

            if (leftNumDocs < rightNumDocs) {
                LOGGER.debug("Right table is larger with {} docs (left has {}. Will change left plan to broadcast its result",
                    rightNumDocs, leftNumDocs);
                return true;
            }
            return false;
        }

        private static boolean isMergePhaseNeeded(Collection<String> executionNodes,
                                                  ResultDescription resultDescription,
                                                  boolean isDistributed) {
            if (!isDistributed && resultDescription.nodeIds().equals(executionNodes)
                && subPlanHashNoLimits(resultDescription)) {
                // If the nested loop is on the same node and there are not limits on the subplan,
                // we don't need a mergePhase to receive requests but can access the RowReceiver
                // of the nestedLoop directly.
                return false;
            }
            return true;
        }
    }

    private static boolean subPlanHashNoLimits(ResultDescription resultDescription) {
        return resultDescription.limit() == TopN.NO_LIMIT && resultDescription.offset() == 0;
    }

    private static MergePhase buildMergePhase(Planner.Context plannerContext,
                                              QueriedRelation relation,
                                              ResultDescription resultDescription,
                                              Collection<String> nlExecutionNodes) {
        List<Projection> projections = Collections.emptyList();
        if (subPlanHashNoLimits(resultDescription) == false) {
            projections = Collections.singletonList(ProjectionBuilder.topNOrEvalIfNeeded(
                resultDescription.limit(),
                resultDescription.offset(),
                resultDescription.numOutputs(),
                resultDescription.streamOutputs()
            ));
        }

        return new MergePhase(
            plannerContext.jobId(),
            plannerContext.nextExecutionPhaseId(),
            "nl-merge",
            resultDescription.nodeIds().size(),
            nlExecutionNodes,
            resultDescription.streamOutputs(),
            projections,
            DistributionInfo.DEFAULT_SAME_NODE,
            PositionalOrderBy.of(relation.querySpec().orderBy().orElse(null), relation.querySpec().outputs())
        );
    }
}
