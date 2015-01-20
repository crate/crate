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

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.crate.analyze.AnalysisMetaData;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.SelectAnalyzedStatement;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.TableRelation;
import io.crate.analyze.where.WhereClauseAnalyzer;
import io.crate.analyze.where.WhereClauseContext;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.Routing;
import io.crate.operation.projectors.TopN;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.PlanNode;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;

import java.util.*;

import static com.google.common.base.MoreObjects.firstNonNull;

public class CrossJoinConsumer implements Consumer {

    private final CrossJoinVisitor visitor;
    private final static FilteringVisitor FILTERING_VISITOR = new FilteringVisitor();
    private final static InputColumnProducer INPUT_COLUMN_PRODUCER = new InputColumnProducer();

    public CrossJoinConsumer(AnalysisMetaData analysisMetaData) {
        visitor = new CrossJoinVisitor(
                analysisMetaData,
                new EvaluatingNormalizer(analysisMetaData.functions(), RowGranularity.CLUSTER, analysisMetaData.referenceResolver()));
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
        private final EvaluatingNormalizer normalizer;

        public CrossJoinVisitor(AnalysisMetaData analysisMetaData, EvaluatingNormalizer normalizer) {
            this.analysisMetaData = analysisMetaData;
            this.normalizer = normalizer;
        }

        @Override
        protected PlannedAnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, ConsumerContext context) {
            return null;
        }

        @SuppressWarnings("ConstantConditions")
        @Override
        public PlannedAnalyzedRelation visitSelectAnalyzedStatement(SelectAnalyzedStatement statement, ConsumerContext context) {
            if (statement.sources().size() < 2) {
                return null;
            }

            // TODO:
            if (statement.querySpec().groupBy() != null) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return null;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return null;
            }

            boolean isSorted = statement.querySpec().orderBy() != null && statement.querySpec().orderBy().isSorted();
            if (isSorted) {
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
            Map<TableRelation, RelationContext> relations = new IdentityHashMap<>(statement.sources().size());
            WhereClause whereClause = statement.querySpec().where();
            Symbol query = null;
            if (whereClause != null) {
                query = whereClause.query();
            }
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

                RelationContext relationContext = new RelationContext();

                if (query != null) {
                    QuerySplitter.SplitQueries splitQueries = QuerySplitter.splitForRelation(query, analyzedRelation);
                    if (splitQueries.relationQuery() == null) {
                        relationContext.whereClause = WhereClause.MATCH_ALL;
                    } else {
                        relationContext.whereClause = new WhereClause(splitQueries.relationQuery());
                        relationContext.whereClause = relationContext.whereClause.normalize(normalizer);
                    }
                    query = splitQueries.remainingQuery();
                } else {
                    relationContext.whereClause = WhereClause.MATCH_ALL;
                }
                FilteringContext filteringContext = filterOutputForRelation(statement.querySpec().outputs(), tableRelation);
                relationContext.outputs = Lists.newArrayList(filteringContext.allOutputs());
                relations.put(tableRelation, relationContext);
            }
            if (query != null && !(query instanceof Literal)) {
                context.validationException(new ValidationException(
                        "WhereClause contains a function or operator that involves more than 1 relation. This is not supported"));
                return null;
            }


            Integer qtfLimit = null;
            int queryLimit = firstNonNull(statement.querySpec().limit(), TopN.NO_LIMIT);
            int queryOffset = firstNonNull(statement.querySpec().offset(), TopN.NO_OFFSET);
            if (statement.querySpec().limit() != null) {
                qtfLimit = queryOffset + queryLimit;
            }
            List<QueryThenFetchNode> queryThenFetchNodes = new ArrayList<>(relations.size());

            for (Map.Entry<TableRelation, RelationContext> entry : relations.entrySet()) {
                RelationContext relationContext = entry.getValue();
                TableRelation tableRelation = entry.getKey();

                WhereClauseAnalyzer whereClauseAnalyzer = new WhereClauseAnalyzer(analysisMetaData, tableRelation);
                WhereClauseContext whereClauseContext = whereClauseAnalyzer.analyze(relationContext.whereClause);
                Routing routing = tableRelation.tableInfo().getRouting(whereClauseContext.whereClause(), null);
                QueryThenFetchNode qtf = new QueryThenFetchNode(
                        routing,
                        tableRelation.resolveCopy(relationContext.outputs),
                        tableRelation.resolveCopy(relationContext.orderBy),
                        new boolean[0],
                        new Boolean[0],
                        qtfLimit,
                        0,
                        whereClauseContext.whereClause(),
                        tableRelation.tableInfo().partitionedByColumns()
                );
                queryThenFetchNodes.add(qtf);
            }
            NestedLoopNode nestedLoopNode = toNestedLoop(queryThenFetchNodes, queryLimit, queryOffset, isSorted);
    
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
            List<Symbol> postOutputs = replaceFieldsWithInputColumns(statement.querySpec().outputs(), relations);
            TopNProjection topNProjection = new TopNProjection(queryLimit, queryOffset);
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
                                                           Map<TableRelation, RelationContext> relations) {
            List<Symbol> result = new ArrayList<>();
            List<Symbol> inputs = new ArrayList<>();
            for (RelationContext relationContext : relations.values()) {
                inputs.addAll(relationContext.outputs);
            }
            for (Symbol statementOutput : statementOutputs) {
                result.add(INPUT_COLUMN_PRODUCER.process(statementOutput, new InputColumnProducerContext(inputs)));
            }
            return result;
        }

        private NestedLoopNode toNestedLoop(List<? extends PlanNode> sourcePlanNodes, int limit, int offset, boolean isSorted) {
            if (sourcePlanNodes.size() == 2) {
                return new NestedLoopNode(
                        sourcePlanNodes.get(0),
                        sourcePlanNodes.get(1),
                        false,
                        limit,
                        offset,
                        isSorted
                );
            } else if (sourcePlanNodes.size() > 2) {
                return new NestedLoopNode(
                        sourcePlanNodes.get(0),
                        toNestedLoop(sourcePlanNodes.subList(1, sourcePlanNodes.size()), limit, offset, isSorted),
                        false,
                        limit,
                        offset,
                        isSorted
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
        List<Symbol> orderBy = new ArrayList<>();
        public WhereClause whereClause;

        public RelationContext() {}
    }

    private static class FilteringContext {

        Stack<Symbol> parents = new Stack<>();
        TableRelation tableRelation;

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
                if (context.parents.isEmpty()) {
                    context.directOutputs.add(field);
                }
                return field;
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
