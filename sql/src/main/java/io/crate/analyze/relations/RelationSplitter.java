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

import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.symbol.*;
import io.crate.metadata.OutputName;

import java.util.*;

public class RelationSplitter {

    private final static SplittingVisitor SPLITTING_VISITOR = new SplittingVisitor();

    public static SplitQuerySpecContext splitQuerySpec(AnalyzedRelation analyzedRelation,
                                                       QuerySpec querySpec) {
        QuerySpec splitQuerySpec = extractQuerySpecForRelation(analyzedRelation, querySpec);

        List<OutputName> outputNames = new ArrayList<>(splitQuerySpec.outputs().size());
        for (Symbol symbol : splitQuerySpec.outputs()) {
            outputNames.add(new OutputName(SymbolFormatter.format(symbol)));
        }

        return new SplitQuerySpecContext(splitQuerySpec, outputNames);
    }

    /**
     * Extracts a query specification of the given relation from a given upper level query specification
     * @param relation the relation for which the query specification should be extracted for
     * @param querySpec the source query specification
     * @return the new query specification holding only fields from the given relation
     */
    public static QuerySpec extractQuerySpecForRelation(AnalyzedRelation relation, QuerySpec querySpec) {
        QuerySpec splitQuerySpec = new QuerySpec();
        List<Symbol> outputs = querySpec.outputs();
        if (outputs == null || outputs.isEmpty()) {
            throw new IllegalArgumentException("a querySpec needs to have some outputs in order to create a new sub-relation");
        }
        List<Symbol> splitOutputs = Lists.newArrayList(RelationSplitter.splitForRelation(relation, outputs).splitSymbols());

        Optional<OrderBy> orderBy = querySpec.orderBy();
        if (orderBy.isPresent()) {
            RelationSplitter.splitOrderBy(relation, querySpec, splitQuerySpec, splitOutputs, orderBy.get());
        }

        WhereClause where = querySpec.where();
        if (where != null && where.hasQuery()) {
            RelationSplitter.splitWhereClause(relation, querySpec, splitQuerySpec, splitOutputs, where);
        } else {
            splitQuerySpec.where(WhereClause.MATCH_ALL);
        }

        // pull down limit = limit + offset, offset=0 by default
        if (querySpec.limit().isPresent()) {
            splitQuerySpec.limit(querySpec.limit().get() + querySpec.offset());
            splitQuerySpec.offset(0);
        }
        splitQuerySpec.outputs(splitOutputs);
        return splitQuerySpec;
    }

    public static void replaceFields(Iterable<? extends QueriedRelation> subRelations,
                                     QuerySpec parentQuerySpec) {
        Map<Symbol, Field> fieldMap = new HashMap<>();
        for (QueriedRelation subRelation : subRelations) {
            List<Field> fields = subRelation.fields();
            QuerySpec splitQuerySpec = subRelation.querySpec();
            for (int i = 0; i < splitQuerySpec.outputs().size(); i++) {
                fieldMap.put(splitQuerySpec.outputs().get(i), fields.get(i));
            }
        }
        replaceFields(parentQuerySpec, fieldMap);
    }

    public static void replaceFields(QuerySpec parentQuerySpec, Map<Symbol, Field> fieldMap){
        MappingSymbolVisitor.inPlace().processInplace(parentQuerySpec.outputs(), fieldMap);
        WhereClause where = parentQuerySpec.where();
        if (where != null && where.hasQuery()) {
            parentQuerySpec.where(new WhereClause(
                    MappingSymbolVisitor.inPlace().process(where.query(), fieldMap)
                    ));
        }
        if (parentQuerySpec.orderBy().isPresent()) {
            MappingSymbolVisitor.inPlace().processInplace(parentQuerySpec.orderBy().get().orderBySymbols(), fieldMap);
        }
    }

    public static void replaceFields(QueriedRelation subRelation,
                                     QuerySpec parentQuerySpec) {
        QuerySpec splitQuerySpec = subRelation.querySpec();
        Map<Symbol, Field> fieldMap = new HashMap<>();
        List<Field> fields = subRelation.fields();
        for (int i = 0; i < splitQuerySpec.outputs().size(); i++) {
            fieldMap.put(splitQuerySpec.outputs().get(i), fields.get(i));
        }
        replaceFields(parentQuerySpec, fieldMap);
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
        } else {
            splitQuerySpec.where(WhereClause.MATCH_ALL);
        }
        SplitContext splitContext = splitForRelation(analyzedRelation, splitQueries.remainingQuery());
        assert splitContext.directSplit.isEmpty();
        splitOutputs.addAll(splitContext.mixedSplit);
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
        addAllNew(splitOutputs, splitContext.directSplit);
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
        OrderBy orderBy = querySpec.orderBy().get();

        boolean[] reverseFlags = new boolean[splitContext.directSplit.size()];
        Boolean[] nullsFirst = new Boolean[splitContext.directSplit.size()];

        int idx = 0;
        for (Symbol symbol : orderBy.orderBySymbols()) {
            int splitIdx = splitContext.directSplit.indexOf(symbol);
            if (splitIdx >= 0 ) {
                reverseFlags[splitIdx] = orderBy.reverseFlags()[idx];
                nullsFirst[splitIdx] = orderBy.nullsFirst()[idx];
            }
            idx++;
        }
        splitQuerySpec.orderBy(new OrderBy(splitContext.directSplit, reverseFlags, nullsFirst));
    }

    private static void addAllNew(List<Symbol> list, Collection<? extends Symbol> collectionToAdd) {
        for (Symbol symbolToAdd : collectionToAdd) {
            if (list.contains(symbolToAdd)) {
                continue;
            }
            list.add(symbolToAdd);
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
