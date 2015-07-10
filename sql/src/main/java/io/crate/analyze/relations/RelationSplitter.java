/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.relations;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.metadata.OutputName;
import io.crate.planner.consumer.QuerySplitter;
import io.crate.planner.symbol.*;

import java.util.*;

public class RelationSplitter {

    private final static SplittingVisitor SPLITTING_VISITOR = new SplittingVisitor();
    private final static FieldReplacingVisitor FIELD_REPLACING_VISITOR = new FieldReplacingVisitor();

    public static SplitQuerySpecContext splitQuerySpec(AnalyzedRelation analyzedRelation,
                                                       QuerySpec querySpec) {
        QuerySpec splitQuerySpec = new QuerySpec();
        List<Symbol> outputs = querySpec.outputs();
        if (outputs == null || outputs.isEmpty()) {
            throw new IllegalArgumentException("a querySpec needs to have some outputs in order to create a new sub-relation");
        }
        List<Symbol> splitOutputs = Lists.newArrayList(RelationSplitter.splitForRelation(analyzedRelation, outputs).splitSymbols());

        OrderBy orderBy = querySpec.orderBy();
        if (orderBy != null) {
            RelationSplitter.splitOrderBy(analyzedRelation, querySpec, splitQuerySpec, splitOutputs, orderBy);
        }

        WhereClause where = querySpec.where();
        if (where != null && where.hasQuery()) {
            RelationSplitter.splitWhereClause(analyzedRelation, querySpec, splitQuerySpec, splitOutputs, where);
        } else {
            splitQuerySpec.where(WhereClause.MATCH_ALL);
        }

        // pull down limit = limit + offset, offset=0 by default
        Integer limit = querySpec.limit();
        if (limit != null) {
            splitQuerySpec.limit(limit + querySpec.offset());
            splitQuerySpec.offset(0);
        }
        splitQuerySpec.outputs(splitOutputs);

        List<OutputName> outputNames = new ArrayList<>(splitQuerySpec.outputs().size());
        for (Symbol symbol : splitQuerySpec.outputs()) {
            outputNames.add(new OutputName(SymbolFormatter.format(symbol)));
        }

        return new SplitQuerySpecContext(splitQuerySpec, outputNames);
    }

    public static void replaceFields(AnalyzedRelation analyzedRelation,
                                     QuerySpec querySpec,
                                     QuerySpec splitQuerySpec) {
        WhereClause where;
        OrderBy orderBy;Map<Symbol, Field> fieldMap = new HashMap<>();
        List<Field> fields = analyzedRelation.fields();
        for (int i = 0; i < splitQuerySpec.outputs().size(); i++) {
            fieldMap.put(splitQuerySpec.outputs().get(i), fields.get(i));
        }
        FieldReplacingCtx fieldReplacingCtx = new FieldReplacingCtx(analyzedRelation, fieldMap);
        replaceFields(querySpec.outputs(), fieldReplacingCtx);

        where = querySpec.where();
        if (where != null && where.hasQuery()) {
            querySpec.where(new WhereClause(replaceFields(where.query(), fieldReplacingCtx)));
        }
        orderBy = querySpec.orderBy();
        if (orderBy != null) {
            replaceFields(orderBy.orderBySymbols(), fieldReplacingCtx);
        }
    }

    private static void splitWhereClause(AnalyzedRelation analyzedRelation,
                                         QuerySpec querySpec,
                                         QuerySpec splitQuerySpec,
                                         List<Symbol> splitOutputs,
                                         WhereClause where) {
        QuerySplitter.SplitQueries splitQueries = QuerySplitter.splitForRelation(analyzedRelation, where.query());
        if (splitQueries.relationQuery() != null) {
            splitQuerySpec.where(new WhereClause(splitQueries.relationQuery()));
            querySpec.where(new WhereClause(splitQueries.remainingQuery()));

            SplitContext splitContext = splitForRelation(analyzedRelation, splitQueries.remainingQuery());
            assert splitContext.directSplit.isEmpty();
            splitOutputs.addAll(splitContext.mixedSplit);
        } else {
            splitQuerySpec.where(WhereClause.MATCH_ALL);
        }
    }

    private static void splitOrderBy(AnalyzedRelation analyzedRelation,
                                     QuerySpec querySpec,
                                     QuerySpec splitQuerySpec,
                                     List<Symbol> splitOutputs,
                                     OrderBy orderBy) {
        SplitContext splitContext = splitForRelation(analyzedRelation, orderBy.orderBySymbols());
        addAllNew(splitOutputs, splitContext.mixedSplit);

        if (!splitContext.directSplit.isEmpty()) {
            rewriteOrderBy(querySpec, splitQuerySpec, splitContext);
        }
    }

    /**
     * sets the (rewritten) orderBy to the parent querySpec and to the splitQuerySpec.
     * E.g. in a query like:
     *
     *  select * from t1, t2 order by t1.x, t2.y
     *
     * Assuming t1 is the current relation for which the QueriedTable is being built.
     *
     * SplitContext will contain 1 directSplit symbol (t1.x)
     *
     * The parent OrderBy is rewritten:
     *
     *  orderBy:      [t1.x, t2.y]      - [t2.y]
     *  reverseFlags: [false, false]    - [false]
     *  nullsFirst:   [null, null]      - [null]
     *
     * The split OrderBy will be set as:
     *
     *  [t1.x]
     *  [false]
     *  [null]
     */
    private static void rewriteOrderBy(QuerySpec querySpec,
                                       QuerySpec splitQuerySpec,
                                       SplitContext splitContext) {
        OrderBy orderBy = querySpec.orderBy();
        assert orderBy != null;

        boolean[] reverseFlags = new boolean[splitContext.directSplit.size()];
        Boolean[] nullsFirst = new Boolean[splitContext.directSplit.size()];

        int numRemaining = orderBy.orderBySymbols().size() - splitContext.directSplit.size();
        List<Symbol> remainingOrderBySymbols = new ArrayList<>(numRemaining);
        boolean[] remainingReverseFlags = new boolean[numRemaining];
        Boolean[] remainingNullsFirst = new Boolean[numRemaining];

        int idx = 0;
        for (Symbol symbol : orderBy.orderBySymbols()) {
            int splitIdx = splitContext.directSplit.indexOf(symbol);
            if (splitIdx < 0) {
                remainingReverseFlags[remainingOrderBySymbols.size()] = orderBy.reverseFlags()[idx];
                remainingNullsFirst[remainingOrderBySymbols.size()] = orderBy.nullsFirst()[idx];
                remainingOrderBySymbols.add(symbol);
            } else {
                reverseFlags[splitIdx] = orderBy.reverseFlags()[idx];
                nullsFirst[splitIdx] = orderBy.nullsFirst()[idx];
            }
            idx++;
        }
        splitQuerySpec.orderBy(new OrderBy(splitContext.directSplit, reverseFlags, nullsFirst));
        querySpec.orderBy(new OrderBy(remainingOrderBySymbols, remainingReverseFlags, remainingNullsFirst));
    }

    private static void addAllNew(List<Symbol> list, Collection<? extends Symbol> collectionToAdd) {
        for (Symbol symbolToAdd : collectionToAdd) {
            if (list.contains(symbolToAdd)) {
                continue;
            }
            list.add(symbolToAdd);
        }
    }


    private static Symbol replaceFields(Symbol symbol, FieldReplacingCtx fieldReplacingCtx) {
        return FIELD_REPLACING_VISITOR.process(symbol, fieldReplacingCtx);
    }

    private static void replaceFields(List<Symbol> outputs, FieldReplacingCtx fieldReplacingCtx) {
        for (int i = 0; i < outputs.size(); i++) {
            outputs.set(i, replaceFields(outputs.get(i), fieldReplacingCtx));
        }
    }

    private static class FieldReplacingCtx {
        AnalyzedRelation relation;
        Map<Symbol, Field> fieldMap;

        public FieldReplacingCtx(AnalyzedRelation analyzedRelation, Map<Symbol, Field> fieldMap) {
            relation = analyzedRelation;
            this.fieldMap = fieldMap;
        }

        public Field get(Symbol symbol) {
            return fieldMap.get(symbol);
        }
    }

    private static class FieldReplacingVisitor extends SymbolVisitor<FieldReplacingCtx, Symbol> {
        @Override
        public Symbol visitFunction(Function symbol, FieldReplacingCtx context) {
            Field field = context.get(symbol);
            if (field != null) {
                return field;
            }
            if (symbol.arguments().isEmpty()) {
                return symbol;
            }
            List<Symbol> newArgs = new ArrayList<>(symbol.arguments().size());
            for (Symbol argument : symbol.arguments()) {
                newArgs.add(process(argument, context));
            }
            return new Function(symbol.info(), newArgs);
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, FieldReplacingCtx context) {
            Field field = context.get(symbol);
            if (field != null) {
                return field;
            }
            return symbol;
        }
    }

    public static class SplitContext {
        final AnalyzedRelation relation;
        final List<Symbol> directSplit = new ArrayList<>();
        final List<Symbol> mixedSplit = new ArrayList<>();
        final Stack<Symbol> parents = new Stack<>();

        public SplitContext(AnalyzedRelation relation) {
            this.relation = relation;
        }

        public Iterable<Symbol> splitSymbols() {
            return FluentIterable.from(directSplit).append(mixedSplit);
        }
    }

    private static SplitContext splitForRelation(AnalyzedRelation relation, Symbol symbol) {
        SplitContext context = new SplitContext(relation);
        SPLITTING_VISITOR.process(symbol, context);
        return context;
    }

    private static SplitContext splitForRelation(AnalyzedRelation relation, Collection<? extends Symbol> symbols) {
        SplitContext context = new SplitContext(relation);
        for (Symbol symbol : symbols) {
            SPLITTING_VISITOR.process(symbol, context);
        }
        return context;
    }

    private static class SplittingVisitor extends SymbolVisitor<SplitContext, Symbol> {

        @Override
        public Symbol visitFunction(Function function, SplitContext context) {
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
                    context.directSplit.add(newFunction);
                }
                return newFunction;
            } else {
                newArg:
                for (Symbol newArg : newArgs) {
                    if ( !(newArg instanceof Function)) {
                        //if ( !(newArg instanceof Function) || ((Function) newArg).info().deterministic()) {
                        for (Symbol symbol : context.splitSymbols()) {
                            if (symbol.equals(newArg)) {
                                break newArg;
                            }
                        }
                    }
                    if (!newArg.symbolType().isValueSymbol()) {
                        context.mixedSplit.add(newArg);
                    }
                }
            }
            return null;
        }

        @Override
        public Symbol visitLiteral(Literal symbol, SplitContext context) {
            return symbol;
        }

        @Override
        public Symbol visitField(Field field, SplitContext context) {
            if (field.relation() == context.relation) {
                if (context.parents.isEmpty()) {
                    context.directSplit.add(field);
                }
                return field;
            }
            return null;
        }
    }

    public static class SplitQuerySpecContext {
        private QuerySpec querySpec;
        private List<OutputName> outputNames;

        public SplitQuerySpecContext(QuerySpec querySpec, List<OutputName> outputNames) {
            this.querySpec = querySpec;
            this.outputNames = outputNames;
        }

        public QuerySpec querySpec() {
            return querySpec;
        }

        public List<OutputName> outputNames() {
            return outputNames;
        }
    }
}
