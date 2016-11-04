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
import io.crate.analyze.*;
import io.crate.analyze.relations.*;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.operation.projectors.TopN;
import io.crate.planner.*;
import io.crate.planner.distribution.DistributionInfo;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.InputCreatingVisitor;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.sql.tree.QualifiedName;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;

class NestedLoopConsumer implements Consumer {

    private final static Logger LOGGER = Loggers.getLogger(NestedLoopConsumer.class);
    private final Visitor visitor;

    NestedLoopConsumer(ClusterService clusterService, Functions functions, TableStats tableStats) {
        visitor = new Visitor(clusterService, functions, tableStats);
    }

    @Override
    public Plan consume(AnalyzedRelation rootRelation, ConsumerContext context) {
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
        private final Functions functions;
        private final TableStats tableStats;

        public Visitor(ClusterService clusterService, Functions functions, TableStats tableStats) {
            this.clusterService = clusterService;
            this.functions = functions;
            this.tableStats = tableStats;
        }

        @Override
        public Plan visitTwoTableJoin(TwoTableJoin statement, ConsumerContext context) {
            QuerySpec querySpec = statement.querySpec();

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
            OrderBy orderByBeforeSplit = querySpec.orderBy().orElse(null);

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

            if (filterNeeded || joinCondition != null || statement.remainingOrderBy().isPresent()) {
                left.querySpec().limit(Optional.empty());
                right.querySpec().limit(Optional.empty());
                left.querySpec().offset(Optional.empty());
                right.querySpec().offset(Optional.empty());
            }

            if (!filterNeeded && joinCondition == null && querySpec.limit().isPresent()) {
                context.requiredPageSize(limits.limitAndOffset());
            }

            // this normalization is required to replace fields of the table relations
            if (left instanceof QueriedTableRelation) {
                ((QueriedTableRelation) left).normalize(
                    functions, context.plannerContext().transactionContext());
            }
            if (right instanceof QueriedTableRelation) {
                ((QueriedTableRelation) right).normalize(
                    functions, context.plannerContext().transactionContext());
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
                broadcastLeftTable = isLeftSmallerThanRight(left, right);
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
            if (isDistributed) {
                leftPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
                nlExecutionNodes = leftResultDesc.nodeIds();
            } else {
                if (isMergePhaseNeeded(nlExecutionNodes, leftResultDesc.nodeIds(), false)) {
                    leftMerge = new MergePhase(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        "nl-merge",
                        leftResultDesc.nodeIds().size(),
                        nlExecutionNodes,
                        leftResultDesc.streamOutputs(),
                        Collections.emptyList(),
                        DistributionInfo.DEFAULT_SAME_NODE,
                        PositionalOrderBy.of(left.querySpec().orderBy().orElse(null), left.querySpec().outputs())
                    );
                }
            }
            if (nlExecutionNodes.size() == 1
                && nlExecutionNodes.equals(rightResultDesc.nodeIds())) {
                // if the left and the right plan are executed on the same single node the mergePhase
                // should be omitted. This is the case if the left and right table have only one shards which
                // are on the same node
                rightPlan.setDistributionInfo(DistributionInfo.DEFAULT_SAME_NODE);
            } else {
                if (isMergePhaseNeeded(nlExecutionNodes, rightResultDesc.nodeIds(), isDistributed)) {
                    rightMerge = new MergePhase(
                        context.plannerContext().jobId(),
                        context.plannerContext().nextExecutionPhaseId(),
                        "nl-merge",
                        rightResultDesc.nodeIds().size(),
                        nlExecutionNodes,
                        rightResultDesc.streamOutputs(),
                        Collections.emptyList(),
                        DistributionInfo.DEFAULT_SAME_NODE,
                        PositionalOrderBy.of(right.querySpec().orderBy().orElse(null), right.querySpec().outputs())
                    );
                }
                rightPlan.setDistributionInfo(DistributionInfo.DEFAULT_BROADCAST);
            }


            if (broadcastLeftTable) {
                Plan tmpPlan = leftPlan;
                leftPlan = rightPlan;
                rightPlan = tmpPlan;
                leftMerge = rightMerge;
                rightMerge = null;
                leftResultDesc = leftPlan.resultDescription();
                rightResultDesc = rightPlan.resultDescription();
            }
            List<Projection> projections = new ArrayList<>();

            if (filterNeeded) {
                InputCreatingVisitor.Context inputVisitorContext = new InputCreatingVisitor.Context(nlOutputs);
                Symbol filterSymbol = InputCreatingVisitor.INSTANCE.process(where.query(), inputVisitorContext);
                assert filterSymbol instanceof Function : "Only function symbols are allowed for filtering";
                projections.add(new FilterProjection(filterSymbol));
            }
            if (joinCondition != null) {
                InputCreatingVisitor.Context inputVisitorContext = new InputCreatingVisitor.Context(nlOutputs);
                joinCondition = InputCreatingVisitor.INSTANCE.process(joinCondition, inputVisitorContext);
                assert joinCondition instanceof Function : "Only function symbols are valid join conditions";
                boolean hasRelCol = SymbolVisitors.any(s -> s instanceof RelationColumn, joinCondition);
                assert  !hasRelCol : "RelationColumns are not valid join condition arguments";
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
            if (isDistributed) {
                return new NestedLoop(
                    nl,
                    leftPlan,
                    rightPlan,
                    limits.finalLimit(),
                    limits.offset(),
                    limit,
                    PositionalOrderBy.of(orderByBeforeSplit, postNLOutputs)
                );
            } else {
                return new NestedLoop(nl, leftPlan, rightPlan, TopN.NO_LIMIT, 0, limit, null);
            }
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
                                                  Collection<String> upstreamPhaseExecutionNodes,
                                                  boolean isDistributed) {
            if (!isDistributed && upstreamPhaseExecutionNodes.equals(executionNodes)) {
                // if the nested loop is on the same node we don't need a mergePhase to receive requests
                // but can access the RowReceiver of the nestedLoop directly
                return false;
            }
            return true;
        }
    }

    private static class SubRelationConverter extends AnalyzedRelationVisitor<RelationSource, QueriedRelation> {

        static final SubRelationConverter INSTANCE = new SubRelationConverter();

        @Override
        public QueriedRelation visitTableRelation(TableRelation tableRelation, RelationSource source) {
            return new QueriedTable(tableRelation, source.querySpec());
        }

        @Override
        public QueriedRelation visitDocTableRelation(DocTableRelation tableRelation, RelationSource source) {
            return new QueriedDocTable(tableRelation, source.querySpec());
        }

        @Override
        public QueriedRelation visitTwoTableJoin(TwoTableJoin twoTableJoin, RelationSource context) {
            return twoTableJoin;
        }

        @Override
        public QueriedRelation visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation,
                                                          RelationSource context) {
            return new QueriedTable(tableFunctionRelation, context.querySpec());
        }

        @Override
        protected QueriedTableRelation visitAnalyzedRelation(AnalyzedRelation relation, RelationSource source) {
            throw new ValidationException("JOIN with sub queries is not supported");
        }
    }
}
