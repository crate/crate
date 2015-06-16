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
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.ValidationException;
import io.crate.operation.projectors.TopN;
import io.crate.planner.PlanNodeBuilder;
import io.crate.planner.node.NoopPlannedAnalyzedRelation;
import io.crate.planner.node.dql.DQLPlanNode;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.planner.node.dql.join.NestedLoopPhase;
import io.crate.planner.projection.FilterProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.Nullable;

import java.util.*;


public class CrossJoinConsumer implements Consumer {

    private final Visitor visitor;
    private final static InputColumnProducer INPUT_COLUMN_PRODUCER = new InputColumnProducer();

    public CrossJoinConsumer(ClusterService clusterService,
                             AnalysisMetaData analysisMetaData,
                             ConsumingPlanner consumingPlanner) {
        visitor = new Visitor(clusterService, analysisMetaData, consumingPlanner);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        return visitor.process(rootRelation, context);
    }

    private static class Visitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final ClusterService clusterService;
        private final AnalysisMetaData analysisMetaData;
        private final ConsumingPlanner consumingPlanner;

        public Visitor(ClusterService clusterService, AnalysisMetaData analysisMetaData, ConsumingPlanner consumingPlanner) {
            this.clusterService = clusterService;
            this.analysisMetaData = analysisMetaData;
            this.consumingPlanner = consumingPlanner;
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
            List<QueriedTable> queriedTables = new ArrayList<>();
            for (Map.Entry<QualifiedName, AnalyzedRelation> entry : statement.sources().entrySet()) {
                AnalyzedRelation analyzedRelation = entry.getValue();
                if (!(analyzedRelation instanceof TableRelation)) {
                    context.validationException(new ValidationException("CROSS JOIN with sub queries is not supported"));
                    return null;
                }
                QueriedTable queriedTable = getQueriedTable(statement, entry.getKey(), (TableRelation) analyzedRelation);
                queriedTables.add(queriedTable);
            }

            WhereClause where = statement.querySpec().where();
            OrderBy orderBy = statement.querySpec().orderBy();
            boolean hasRemainingQuery = where.hasQuery() && !(where.query() instanceof Literal);
            boolean hasRemainingOrderBy = orderBy != null && orderBy.isSorted();
            if (hasRemainingQuery || hasRemainingOrderBy) {
                for (QueriedTable queriedTable : queriedTables) {
                    queriedTable.querySpec().limit(null);
                    queriedTable.querySpec().offset(TopN.NO_OFFSET);
                }
            }
            sortQueriedTables(relationOrder, queriedTables);

            // TODO: replace references with docIds.. and add fetch projection

            NestedLoop nl = toNestedLoop(queriedTables, context);
            List<Symbol> queriedTablesOutputs = getAllOutputs(queriedTables);

            ImmutableList.Builder<Projection> projectionBuilder = ImmutableList.builder();
            if (hasRemainingQuery) {
                Symbol filter = replaceFieldsWithInputColumns(where.query(), queriedTablesOutputs);
                projectionBuilder.add(new FilterProjection(filter));
            }
            /**
             * TopN for:
             *
             * #1 Reorder
             *      need to always use topN to re-order outputs,
             *
             *      e.g. select t1.name, t2.name, t1.id
             *
             *      left outputs:
             *          [ t1.name, t1.id ]
             *
             *      right outputs:
             *          [ t2.name ]
             *
             *      left + right outputs:
             *          [ t1.name, t1.id, t2.name]
             *
             *      final outputs (topN):
             *          [ in(0), in(2), in(1)]
             *
             * #2 Execute functions that reference more than 1 relations
             *
             *      select t1.x + t2.x
             *
             *      left: x
             *      right: x
             *
             *      topN:  add(in(0), in(1))
             *
             * #3 Apply Limit and remaining Order by
             */
            List<Symbol> postOutputs = replaceFieldsWithInputColumns(statement.querySpec().outputs(), queriedTablesOutputs);
            TopNProjection topNProjection;

            int rootLimit = MoreObjects.firstNonNull(statement.querySpec().limit(), Constants.DEFAULT_SELECT_LIMIT);
            if (orderBy != null && orderBy.isSorted()) {
                 topNProjection = new TopNProjection(
                         rootLimit,
                         statement.querySpec().offset(),
                         replaceFieldsWithInputColumns(orderBy.orderBySymbols(), queriedTablesOutputs),
                         orderBy.reverseFlags(),
                         orderBy.nullsFirst()
                 );
            } else {
                topNProjection = new TopNProjection(rootLimit, statement.querySpec().offset());
            }
            topNProjection.outputs(postOutputs);
            projectionBuilder.add(topNProjection);

            ImmutableList<Projection> projections = projectionBuilder.build();
            for (Projection projection : projections) {
                nl.localMergePhase().addProjection(projection);
            }
            return nl;
        }

        private List<Symbol> getAllOutputs(Collection<QueriedTable> queriedTables) {
            ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
            for (QueriedTable table : queriedTables) {
                builder.addAll(table.fields());
            }
            return builder.build();
        }
        /**
         * generates new symbols that will use InputColumn symbols to point to the output of the given relations
         *
         * @param statementOutputs: [ u1.id,  add(u1.id, u2.id) ]
         * @param inputSymbols:
         * {
         *     [ u1.id, u2.id ],
         * }
         *
         * @return [ in(0), add( in(0), in(1) ) ]
         */
        private List<Symbol> replaceFieldsWithInputColumns(Collection<? extends Symbol> statementOutputs,
                                                           List<Symbol> inputSymbols) {
            List<Symbol> result = new ArrayList<>();
            for (Symbol statementOutput : statementOutputs) {
                result.add(replaceFieldsWithInputColumns(statementOutput, inputSymbols));
            }
            return result;
        }

        private Symbol replaceFieldsWithInputColumns(Symbol symbol, List<Symbol> inputSymbols) {
            return INPUT_COLUMN_PRODUCER.process(symbol, new InputColumnProducerContext(inputSymbols));
        }

        private NestedLoop toNestedLoop(List<QueriedTable> queriedTables, ConsumerContext context) {
            Iterator<QueriedTable> iterator = queriedTables.iterator();
            NestedLoop nl = null;
            PlannedAnalyzedRelation left;
            PlannedAnalyzedRelation right;

            Set<String> localExecutionNodes = ImmutableSet.of(clusterService.localNode().id());
            UUID jobId = context.plannerContext().jobId();

            while (iterator.hasNext()) {
                QueriedTable next = iterator.next();

                if (nl == null) {
                    assert iterator.hasNext();
                    QueriedTable second = iterator.next();

                    left = consumingPlanner.plan(next, context);
                    right = consumingPlanner.plan(second, context);
                    assert left != null && right != null;

                    MergePhase leftMerge = mergePhase(
                            context,
                            localExecutionNodes,
                            jobId,
                            left.resultNode(),
                            next.querySpec().outputs(),
                            next.querySpec().orderBy()
                    );
                    MergePhase rightMerge = mergePhase(
                            context,
                            localExecutionNodes,
                            jobId,
                            right.resultNode(),
                            second.querySpec().outputs(),
                            second.querySpec().orderBy()
                    );
                    NestedLoopPhase nestedLoopPhase = new NestedLoopPhase(
                            jobId,
                            context.plannerContext().nextExecutionPhaseId(),
                            "nested-loop",
                            ImmutableList.<Projection>of(),
                            leftMerge,
                            rightMerge,
                            localExecutionNodes
                    );
                    MergePhase localMerge = PlanNodeBuilder.localMerge(
                            jobId, ImmutableList.<Projection>of(), nestedLoopPhase, context.plannerContext());
                    localMerge.executionNodes(localExecutionNodes);

                    nl = new NestedLoop(jobId, left, right, nestedLoopPhase, true, localMerge);
                } else {
                    throw new UnsupportedOperationException("Cross join with more than 2 tables is not supported");
                    /*
                    NestedLoop lastNL = nl;
                    right = consumingPlanner.plan(next, context);
                    assert right != null;

                    NestedLoopPhase nestedLoopPhase = new NestedLoopPhase(
                            jobId,
                            context.plannerContext().nextExecutionPhaseId(),
                            "nested-loop",
                            ImmutableList.<Projection>of(),
                            ((MergePhase) lastNL.resultNode()),
                            ((MergePhase) right.resultNode()),
                            localExecutionNodes
                    );
                    nl = new NestedLoop(jobId, lastNL, right, nestedLoopPhase, true, null);
                    */
                }
            }
            return nl;
        }

        private MergePhase mergePhase(ConsumerContext context,
                                      Set<String> localExecutionNodes,
                                      UUID jobId,
                                      DQLPlanNode leftDQL,
                                      List<Symbol> previousOutputs,
                                      @Nullable OrderBy orderBy) {
            if (leftDQL instanceof MergePhase && leftDQL.executionNodes().isEmpty()) {
                ((MergePhase) leftDQL).executionNodes(localExecutionNodes);
            }

            MergePhase mergePhase;
            if (OrderBy.isSorted(orderBy)) {
                mergePhase = PlanNodeBuilder.sortedLocalMerge(
                        jobId,
                        ImmutableList.<Projection>of(),
                        orderBy,
                        previousOutputs,
                        orderBy.orderBySymbols(),
                        leftDQL,
                        context.plannerContext()
                );
            } else {
                mergePhase = PlanNodeBuilder.localMerge(
                        jobId,
                        ImmutableList.<Projection>of(),
                        leftDQL,
                        context.plannerContext());
            }
            mergePhase.executionNodes(localExecutionNodes);
            return mergePhase;
        }

        private void sortQueriedTables(final Map<Object, Integer> relationOrder, List<QueriedTable> queriedTables) {
            Collections.sort(queriedTables, new Comparator<QueriedTable>() {
                @Override
                public int compare(QueriedTable o1, QueriedTable o2) {
                    return Integer.compare(
                            MoreObjects.firstNonNull(relationOrder.get(o1.tableRelation()), Integer.MAX_VALUE),
                            MoreObjects.firstNonNull(relationOrder.get(o2.tableRelation()), Integer.MAX_VALUE));
                }
            });
        }

        private QueriedTable getQueriedTable(MultiSourceSelect statement,
                                             QualifiedName relationName,
                                             TableRelation tableRelation) {
            QueriedTable queriedTable = QueriedTable.newSubRelation(relationName, tableRelation, statement.querySpec());
            queriedTable.normalize(analysisMetaData);
            return queriedTable;
        }

        /**
         * returns a map with the relation as keys and the values are their order in occurrence in the order by clause.
         *
         * e.g. select * from t1, t2, t3 order by t2.x, t3.y
         *
         * returns: {
         *     t2: 0                 (first)
         *     t3: 1                 (second)
         *     t1: Integer.MAX_VALUE (last)
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
                    if (relationCount != null && relationCount.numOther == 0 && relationCount.numThis > 0) {
                        orderByOrder.put(analyzedRelation, idx);
                    } else {
                        orderByOrder.put(analyzedRelation, Integer.MAX_VALUE);
                    }
                }
                idx++;
            }
            return orderByOrder;
        }

        private boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.sources().size() < 2) {
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
            return false;
        }

    }

    private static class InputColumnProducerContext {

        private List<Symbol> inputs;

        public InputColumnProducerContext(List<Symbol> inputs) {
            this.inputs = inputs;
        }
    }

    private static class InputColumnProducer extends SymbolVisitor<InputColumnProducerContext, Symbol> {

        @Override
        public Symbol visitFunction(Function function, InputColumnProducerContext context) {
            int idx = 0;
            for (Symbol input : context.inputs) {
                if (input.equals(function)) {
                    return new InputColumn(idx, input.valueType());
                }
                idx++;
            }
            List<Symbol> newArgs = new ArrayList<>(function.arguments().size());
            for (Symbol argument : function.arguments()) {
                newArgs.add(process(argument, context));
            }
            return new Function(function.info(), newArgs);
        }

        @Override
        public Symbol visitField(Field field, InputColumnProducerContext context) {
            int idx = 0;
            for (Symbol input : context.inputs) {
                if (input.equals(field)) {
                    return new InputColumn(idx, input.valueType());
                }
                idx++;
            }
            return field;
        }

        @Override
        public Symbol visitLiteral(Literal literal, InputColumnProducerContext context) {
            return literal;
        }
    }
}
