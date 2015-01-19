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
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.Routing;
import io.crate.operation.projectors.TopN;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;

import java.util.*;

public class CrossJoinConsumer implements Consumer {

    private final CrossJoinVisitor visitor;
    private final static FilteringVisitor FILTERING_VISITOR = new FilteringVisitor();
    private final static PostOutputGenerator POST_OUTPUT_GENERATOR = new PostOutputGenerator();

    public CrossJoinConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new CrossJoinVisitor(analysisMetaData);
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

        private final AnalysisMetaData analysisMetaData;

        public CrossJoinVisitor(AnalysisMetaData analysisMetaData) {
            this.analysisMetaData = analysisMetaData;
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

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

            WhereClause where = MoreObjects.firstNonNull(statement.querySpec().where(), WhereClause.MATCH_ALL);
            if (!where.equals(WhereClause.MATCH_ALL)) {
                context.validationException(new ValidationException("Filtering on CROSS JOIN is not supported"));
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

            boolean hasMixedOutputs = false;

            Map<TableRelation, RelationContext> relations = new IdentityHashMap<>(statement.sources().size());
            for (AnalyzedRelation analyzedRelation : statement.sources().values()) {
                if (!(analyzedRelation instanceof TableRelation)) {
                    context.validationException(new ValidationException("CROSS JOIN with sub queries is not supported"));
                    return null;
                }
                TableRelation tableRelation = (TableRelation) analyzedRelation;
                if (tableRelation.tableInfo().schemaInfo().systemSchema()) {
                    context.validationException(new ValidationException("CROSS JOIN on system tables is not supported"));
                    return null;
                }

                FilteringContext filteringContext = filterOutputForRelation(statement.querySpec().outputs(), tableRelation);
                hasMixedOutputs = hasMixedOutputs || filteringContext.hasMixedOutputs;
                relations.put(tableRelation, new RelationContext(Lists.newArrayList(filteringContext.allOutputs())));
            }

            Integer qtfLimit = null;
            Integer querySpecLimit = statement.querySpec().limit();
            if (querySpecLimit != null) {
                qtfLimit = querySpecLimit + statement.querySpec().offset();
            }
            List<QueryThenFetchNode> queryThenFetchNodes = new ArrayList<>(relations.size());

            for (Map.Entry<TableRelation, RelationContext> entry : relations.entrySet()) {
                RelationContext relationContext = entry.getValue();
                TableRelation tableRelation = entry.getKey();

                WhereClause whereClause = WhereClause.MATCH_ALL;
                Routing routing = tableRelation.tableInfo().getRouting(whereClause, null);
                QueryThenFetchNode qtf = new QueryThenFetchNode(
                        routing,
                        relationContext.outputs,
                        relationContext.orderBy,
                        new boolean[0],
                        new Boolean[0],
                        qtfLimit,
                        0,
                        whereClause,
                        tableRelation.tableInfo().partitionedByColumns()
                );
                queryThenFetchNodes.add(qtf);
            }
            int limit = MoreObjects.firstNonNull(statement.querySpec().limit(), TopN.NO_LIMIT);
            NestedLoopNode nestedLoopNode = toNestedLoop(queryThenFetchNodes, limit, statement.querySpec().offset());

            if (hasMixedOutputs || statement.querySpec().isLimited()) {
                List<Symbol> postOutputs = generatePostOutputs(statement.querySpec().outputs(), relations);
                TopNProjection topNProjection = new TopNProjection(limit, statement.querySpec().offset());
                topNProjection.outputs(postOutputs);
                nestedLoopNode.projections(ImmutableList.<Projection>of(topNProjection));
            }

            return nestedLoopNode;
        }

        /**
         * generates new outputs that can be used in the first projection of the NestedLoopNode which has functions
         * re-written - Example:
         *
         * @param statementOutputs: [ u1.id,  add(u1.id, u2.id) ]
         * @param relations:
         * {
         *     u1: [ id ],
         *     u2: [ id ]
         * }
         *
         * @return [ in(0), add( in(0), in(1) ) ]
         */
        private List<Symbol> generatePostOutputs(List<Symbol> statementOutputs,
                                                 Map<TableRelation, RelationContext> relations) {
            List<Symbol> result = new ArrayList<>();
            for (Symbol statementOutput : statementOutputs) {
                result.add(POST_OUTPUT_GENERATOR.process(statementOutput, new PostOutputContext(relations)));
            }
            return result;
        }

        private NestedLoopNode toNestedLoop(List<? extends PlanNode> sourcePlanNodes, int limit, int offset) {
            if (sourcePlanNodes.size() == 2) {
                return new NestedLoopNode(
                        sourcePlanNodes.get(0),
                        sourcePlanNodes.get(1),
                        false,
                        limit,
                        offset
                );
            } else if (sourcePlanNodes.size() > 2) {
                return new NestedLoopNode(
                        sourcePlanNodes.get(0),
                        toNestedLoop(sourcePlanNodes.subList(1, sourcePlanNodes.size()), limit, offset),
                        false,
                        limit,
                        offset
                );
            }
            return null;
        }


        /**
         * creates a FilterContext which contains the symbols which can be fetched directly from the tableRelation
         */
        private FilteringContext filterOutputForRelation(List<Symbol> symbols, TableRelation tableRelation) {
            FilteringContext context = new FilteringContext(tableRelation);
            for (Symbol symbol : symbols) {
                FILTERING_VISITOR.process(symbol, context);
            }
            return context;
        }
    }

    private static class RelationContext {
        List<Symbol> outputs;
        List<Symbol> orderBy;

        public RelationContext(List<Symbol> outputs) {
            this.outputs = outputs;
        }
    }

    private static class FilteringContext {

        Stack<Symbol> parents = new Stack<>();
        TableRelation tableRelation;

        /**
         * need to track this separate as mixedOutputs might be empty if all columns that are "mixed" are already selected on top level
         *
         * e.g.   select t1.x, t2.x, t1.x + t2.x
         */
        boolean hasMixedOutputs = false;


        /**
         * symbols that belong to tableRelation but are inside a function that contains fields from another relation
         */
        List<Symbol> mixedOutputs = new ArrayList<>();

        /**
         * symbol or functions that belong to tableRelation and can be retrieved/evaluated by the tableRelation/QTF
         */
        List<Symbol> directOutputs = new ArrayList<>();

        public FilteringContext(TableRelation tableRelation) {
            this.tableRelation = tableRelation;
        }

        public Iterable<Symbol> allOutputs() {
            return FluentIterable.from(mixedOutputs).append(directOutputs);
        }
    }

    private static class FilteringVisitor extends SymbolVisitor<FilteringContext, Symbol> {

        @Override
        public Symbol visitFunction(Function function, FilteringContext context) {
            List<Symbol> newArgs = new ArrayList<>(function.arguments().size());
            context.parents.push(function);
            for (Symbol argument : function.arguments()) {
                Symbol processed = process(argument, context);
                if (processed == null) {
                    continue;
                }
                newArgs.add(processed);
            }
            context.parents.pop();

            if (newArgs.size() == function.arguments().size()) {
                Function newFunction = new Function(function.info(), newArgs);
                if (context.parents.isEmpty()) {
                    context.directOutputs.add(newFunction);
                }
                return newFunction;
            } else {
                context.hasMixedOutputs = context.hasMixedOutputs || newArgs.size() > 0;

                newArg:
                for (Symbol newArg : newArgs) {
                    if ( !(newArg instanceof Function) || ((Function) newArg).info().deterministic()) {
                        for (Symbol symbol : context.allOutputs()) {
                            if (symbol.equals(newArg)) {
                                break newArg;
                            }
                        }
                    }
                    context.mixedOutputs.add(newArg);
                }
            }
            return null;
        }

        @Override
        public Symbol visitLiteral(Literal symbol, FilteringContext context) {
            return symbol;
        }

        @Override
        public Symbol visitField(Field field, FilteringContext context) {
            if (field.relation() == context.tableRelation) {
                Reference reference = context.tableRelation.resolveField(field);
                if (context.parents.isEmpty()) {
                    context.directOutputs.add(reference);
                }
                return reference;
            }
            return null;
        }
    }

    private static class PostOutputContext {

        private final Map<TableRelation, RelationContext> relations;

        public PostOutputContext(Map<TableRelation, RelationContext> relations) {
            this.relations = relations;
        }
    }

    private static class PostOutputGenerator extends SymbolVisitor<PostOutputContext, Symbol> {

        @Override
        public Symbol visitFunction(Function function, PostOutputContext context) {
            int idx = 0;
            for (Map.Entry<TableRelation, RelationContext> entry : context.relations.entrySet()) {
                RelationContext relationContext = entry.getValue();
                for (Symbol output : relationContext.outputs) {
                    if (output.equals(function)) {
                        return new InputColumn(idx, output.valueType());
                    }

                    idx++;
                }
            }
            List<Symbol> newArgs = new ArrayList<>(function.arguments().size());
            for (Symbol argument : function.arguments()) {
                newArgs.add(process(argument, context));
            }
            return new Function(function.info(), newArgs);
        }

        @Override
        public Symbol visitField(Field field, PostOutputContext context) {
            int idx = 0;
            for (Map.Entry<TableRelation, RelationContext> entry : context.relations.entrySet()) {
                TableRelation tableRelation = entry.getKey();
                RelationContext relationContext = entry.getValue();
                if (field.relation() != tableRelation) {
                    idx += relationContext.outputs.size();
                    continue;
                }

                Reference reference = tableRelation.resolveField(field);
                for (Symbol output : relationContext.outputs) {
                    if (output.equals(reference)) {
                        return new InputColumn(idx, output.valueType());
                    }
                    idx++;
                }
            }
            return field;
        }

        @Override
        public Symbol visitLiteral(Literal literal, PostOutputContext context) {
            return literal;
        }
    }
}
