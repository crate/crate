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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.operation.operator.AndOperator;
import io.crate.planner.symbol.*;

import javax.annotation.Nullable;
import java.util.*;

public class QuerySplitter {

    private final static InternalSplitter INTERNAL_SPLITTER = new InternalSplitter();
    private final static RelationFieldCounter RELATION_FIELD_COUNTER = new RelationFieldCounter();

    public static class SplitQueries {
        private Symbol relationQuery;
        private Symbol remainingQuery;

        private SplitQueries() {}

        /**
         *
         * @return the query for the given relation. Null if no part could be split-off
         */
        @Nullable
        public Symbol relationQuery() {
            return relationQuery;
        }

        public Symbol remainingQuery() {
            return remainingQuery;
        }
    }

    /**
     * splits the given query into two queries. One that can be executed by the given relation and another one which
     * contains all the remaining parts that can't be executed.
     */
    public static SplitQueries splitForRelation(AnalyzedRelation analyzedRelation, Symbol query) {
        RelationFieldCounterCtx relationFieldCounterCtx = new RelationFieldCounterCtx(analyzedRelation);
        RELATION_FIELD_COUNTER.process(query, relationFieldCounterCtx);

        InternalSplitterCtx internalSplitterCtx = new InternalSplitterCtx(analyzedRelation, relationFieldCounterCtx.countMap);
        Symbol remainingQuery = INTERNAL_SPLITTER.process(query, internalSplitterCtx);

        SplitQueries splitQueries = new SplitQueries();
        splitQueries.remainingQuery = remainingQuery;
        if (!internalSplitterCtx.relationQueryParts.isEmpty()) {
            splitQueries.relationQuery = joinQueryParts(internalSplitterCtx.relationQueryParts);
        }
        return splitQueries;
    }

    public static RelationCount getRelationCount(AnalyzedRelation analyzedRelation, Symbol symbol) {
        RelationFieldCounterCtx relationFieldCounterCtx = new RelationFieldCounterCtx(analyzedRelation);
        RELATION_FIELD_COUNTER.process(symbol, relationFieldCounterCtx);
        return relationFieldCounterCtx.countMap.get(symbol);
    }

    private static Symbol joinQueryParts(List<Symbol> queryParts) {
        if (queryParts.size() == 1) {
            return queryParts.get(0);
        }
        if (queryParts.size() == 2) {
            return new Function(AndOperator.INFO, queryParts);
        }
        return new Function(AndOperator.INFO,
                Arrays.asList(
                        queryParts.get(0),
                        joinQueryParts(queryParts.subList(1, queryParts.size()))));
    }

    public static class RelationFieldCounterCtx {
        AnalyzedRelation analyzedRelation;
        Stack<Symbol> parents = new Stack<>();
        IdentityHashMap<Symbol, RelationCount> countMap = new IdentityHashMap<>();

        public RelationFieldCounterCtx(AnalyzedRelation analyzedRelation) {
            this.analyzedRelation = analyzedRelation;
        }
    }

    public static class RelationCount {
        int numThis = 0;
        int numOther = 0;

        private RelationCount() {
        }

        private RelationCount(int numThis, int numOther) {
            this.numThis = numThis;
            this.numOther = numOther;
        }
    }

    /**
     * A Visitor that will generate a map with information on how many symbols which reference other relations
     * are beneath a symbol.
     *
     * E.g.
     *    where ( t1.x = 1 or t1.x = 2)  and (t2.x = 3 or (t2.x = 4 or t2.x = 5))
     *
     *
     *                          AND 1
     *              OR 2                         OR 3
     *    t1.x = 1    t1.x = 2         t2.x = 3         OR 4
     *                                        t2.x = 4      t2.x = 5
     *
     * For t1:
     * AND 1
     *      numThis: 2
     *      numOther: 3
     *
     * OR 2:
     *      numThis: 2
     *      numOther: 0
     *
     * and so on..
     */
    private static class RelationFieldCounter extends SymbolVisitor<RelationFieldCounterCtx, RelationCount> {

        @Override
        public RelationCount visitFunction(Function function, RelationFieldCounterCtx context) {
            RelationCount relationCount = new RelationCount();
            context.parents.push(function);
            for (Symbol argument : function.arguments()) {
                RelationCount childCounts = process(argument, context);

                relationCount.numOther += childCounts.numOther;
                relationCount.numThis += childCounts.numThis;
            }
            context.parents.pop();
            context.countMap.put(function, relationCount);
            return relationCount;
        }

        @Override
        public RelationCount visitField(Field field, RelationFieldCounterCtx context) {
            RelationCount relationCount;
            if (field.relation() == context.analyzedRelation) {
                relationCount = new RelationCount(1, 0);
            } else {
                relationCount = new RelationCount(0, 1);
            }
            if (context.parents.isEmpty()) {
                context.countMap.put(field, relationCount);
            }
            return relationCount;
        }

        @Override
        public RelationCount visitMatchPredicate(MatchPredicate matchPredicate, RelationFieldCounterCtx context) {
            int numThis = 0;
            int numOther = 0;
            for (Field field : matchPredicate.identBoostMap().keySet()) {
                if (field.relation() == context.analyzedRelation) {
                    numThis++;
                } else {
                    numOther++;
                }
            }
            if (numOther > 0 && numThis > 0) {
                throw new IllegalArgumentException("Must not use columns from more than 1 relation inside the MATCH predicate");
            }
            RelationCount relationCount = new RelationCount(numThis, numOther);
            if (context.parents.isEmpty()) {
                context.countMap.put(matchPredicate, relationCount);
            }
            return relationCount;
        }

        @Override
        protected RelationCount visitSymbol(Symbol symbol, RelationFieldCounterCtx context) {
            RelationCount relationCount = new RelationCount(0, 0);
            if (context.parents.isEmpty()) {
                context.countMap.put(symbol, relationCount);
            }
            return relationCount;
        }
    }

    private static class InternalSplitterCtx {
        final IdentityHashMap<Symbol, RelationCount> countMap;
        final AnalyzedRelation relation;

        List<Symbol> relationQueryParts = new ArrayList<>();

        boolean insideFunction = false;

        public InternalSplitterCtx(AnalyzedRelation relation, IdentityHashMap<Symbol, RelationCount> countMap) {
            this.relation = relation;
            this.countMap = countMap;
        }
    }

    /**
     * Uses the Information generated by the RelationFieldCounter to split the query.
     */
    private static class InternalSplitter extends SymbolVisitor<InternalSplitterCtx, Symbol> {

        @Override
        public Symbol visitFunction(Function function, InternalSplitterCtx context) {
            RelationCount relationCount = context.countMap.get(function);
            if (relationCount.numOther == 0) {
                context.relationQueryParts.add(function);
                return Literal.newLiteral(true);
            } else {
                if (AndOperator.NAME.equals(function.info().ident().name())) {
                    List<Symbol> newArgs = new ArrayList<>(function.arguments().size());
                    for (Symbol argument : function.arguments()) {
                        if (!argument.symbolType().isValueSymbol()) {
                            RelationCount argumentCount = context.countMap.get(argument);
                            assert argumentCount != null : "relationCount for argument must be available";
                            if (argumentCount.numOther == 0) {
                                context.relationQueryParts.add(argument);
                                argument = Literal.newLiteral(true);
                            } else {
                                context.insideFunction = true;
                                argument = process(argument, context);
                                context.insideFunction = false;
                            }
                        }
                        newArgs.add(argument);
                    }
                    return new Function(function.info(), newArgs);
                } else {
                    return function;
                }
            }
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, InternalSplitterCtx context) {
            // for cases like: "where t1.bool_field"  or  "where match (t1.name...)"
            if (!context.insideFunction) {
                RelationCount relationCount = context.countMap.get(symbol);
                assert relationCount != null : "relation count must be available";

                if (relationCount.numOther == 0) {
                    context.relationQueryParts.add(symbol);
                    return Literal.newLiteral(true);
                }
            }
            return symbol;
        }
    }
}
