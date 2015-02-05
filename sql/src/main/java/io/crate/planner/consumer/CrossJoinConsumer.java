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
import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.exceptions.ValidationException;
import io.crate.operation.projectors.TopN;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;



public class CrossJoinConsumer implements Consumer {

    private final CrossJoinVisitor visitor;
    private final static InputColumnProducer INPUT_COLUMN_PRODUCER = new InputColumnProducer();

    public CrossJoinConsumer(ConsumingPlanner consumingPlanner, AnalysisMetaData analysisMetaData) {
        visitor = new CrossJoinVisitor(consumingPlanner, analysisMetaData);
    }

    @Override
    public boolean consume(AnalyzedRelation rootRelation, ConsumerContext context) {
        AnalyzedRelation analyzedRelation = visitor.process(rootRelation, context);
        if (analyzedRelation != null) {
            context.rootRelation(analyzedRelation);
            return true;
        }
        return false;
    }

    private static class CrossJoinVisitor extends AnalyzedRelationVisitor<ConsumerContext, PlannedAnalyzedRelation> {

        private final ConsumingPlanner consumingPlanner;
        private final AnalysisMetaData analysisMetaData;

        public CrossJoinVisitor(ConsumingPlanner consumingPlanner, AnalysisMetaData analysisMetaData) {
            this.consumingPlanner = consumingPlanner;
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

        @SuppressWarnings("ConstantConditions")
        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.sources().size() < 2) {
                return null;
            }

            List<Symbol> groupBy = statement.querySpec().groupBy();
            if (groupBy != null && !groupBy.isEmpty()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return null;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return null;
            }

            OrderBy orderBy = statement.querySpec().orderBy();
            if (orderBy != null && orderBy.isSorted()) {
                context.validationException(new ValidationException("Query with CROSS JOIN doesn't support ORDER BY"));
                return null;
            }

            /**
             * Example statement:
             *
             *      select
             *          t1.name,
             *          t2.x + cast(substr(t3.foo, 1, 1) as integer) - t1.y
             *      from t1, t2, t3
             *      order by
             *          t3.x,
             *          t1.x + t2.x
             *
             * Generate map with outputs per relation:
             *
             *      qtf t1:
             *          outputs: [name, y, x]   // x is included because of order by t1.x + t2.x
             *                                  // need to execute order by in topN projection
             *
             *      qtf t2:
             *          outputs: [x]
             *
             *      qtf t3:
             *          outputs: [foo]          // order by t3.x not included in output, QTF result will be pre-sorted
             *
             * root-nestedLoop:
             *  outputs: [t1.name, t1.y, t2.x, cast(substr(3.foo, 1, 1) as integer)]
             *           [  0,      1,    2,      3                                ]
             *
             * postOutputs: [in(0), subtract( add( in(1), in(3)), in(1) ]
             */

            WhereClause where = MoreObjects.firstNonNull(statement.querySpec().where(), WhereClause.MATCH_ALL);
            List<Plan> nestedPlans = new ArrayList<>();
            List<QueriedTable> queriedTables = new ArrayList<>();

            for (Map.Entry<QualifiedName, AnalyzedRelation> entry : statement.sources().entrySet()) {
                AnalyzedRelation analyzedRelation = entry.getValue();
                if (!(analyzedRelation instanceof TableRelation)) {
                    context.validationException(new ValidationException("CROSS JOIN with sub queries is not supported"));
                    return null;
                }
                TableRelation tableRelation = (TableRelation) analyzedRelation;
                QueriedTable queriedTable = QueriedTable.newSubRelation(entry.getKey(), tableRelation, statement.querySpec());
                queriedTable.normalize(analysisMetaData);
                queriedTables.add(queriedTable);
                where = statement.querySpec().where();
                nestedPlans.add(consumingPlanner.plan(queriedTable));
            }
            if (where.hasQuery() && !(where.query() instanceof Literal)) {
                context.validationException(new ValidationException(
                        "WhereClause contains a function or operator that involves more than 1 relation. This is not supported"));
                return null;
            }

            int limit = MoreObjects.firstNonNull(statement.querySpec().limit(), TopN.NO_LIMIT);
            NestedLoopNode nestedLoopNode = toNestedLoop(nestedPlans, limit, statement.querySpec().offset());

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
             * #3 Apply Limit (and Order by once supported..)
             */
            List<Symbol> postOutputs = replaceFieldsWithInputColumns(statement.querySpec().outputs(), queriedTables);
            TopNProjection topNProjection = new TopNProjection(limit, statement.querySpec().offset());
            topNProjection.outputs(postOutputs);
            nestedLoopNode.projections(ImmutableList.<Projection>of(topNProjection));
            nestedLoopNode.outputTypes(Symbols.extractTypes(postOutputs));
            return nestedLoopNode;
        }

        /**
         * generates new symbols that will use InputColumn symbols to point to the output of the given relations
         *
         * @param statementOutputs: [ u1.id,  add(u1.id, u2.id) ]
         * @param relations:
         * {
         *     u1: [ u1.id ],
         *     u2: [ u2.id ]
         * }
         *
         * @return [ in(0), add( in(0), in(1) ) ]
         */
        private List<Symbol> replaceFieldsWithInputColumns(List<Symbol> statementOutputs,
                                                           List<QueriedTable> relations) {
            List<Symbol> result = new ArrayList<>();
            List<Symbol> inputs = new ArrayList<>();
            for (QueriedTable queriedTable : relations) {
                inputs.addAll(queriedTable.fields());
            }
            for (Symbol statementOutput : statementOutputs) {
                result.add(INPUT_COLUMN_PRODUCER.process(statementOutput, new InputColumnProducerContext(inputs)));
            }
            return result;
        }

        private NestedLoopNode toNestedLoop(List<Plan> subRelationPlans, int limit, int offset) {
            if (subRelationPlans.size() == 2) {
                return new NestedLoopNode(
                        subRelationPlans.get(0),
                        subRelationPlans.get(1),
                        false,
                        limit,
                        offset
                );
            } else if (subRelationPlans.size() > 2) {
                Plan nestedLoopPlan = new IterablePlan(
                        toNestedLoop(subRelationPlans.subList(1, subRelationPlans.size()), limit, offset)
                );
                return new NestedLoopNode(
                        subRelationPlans.get(0),
                        nestedLoopPlan,
                        false,
                        limit,
                        offset
                );
            }
            return null;
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
