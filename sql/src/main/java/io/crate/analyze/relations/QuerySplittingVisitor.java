/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.relations;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import io.crate.analyze.symbol.*;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.operation.operator.AndOperator;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

class QuerySplittingVisitor extends ReplacingSymbolVisitor<QuerySplittingVisitor.Context> {

    public static final QuerySplittingVisitor INSTANCE = new QuerySplittingVisitor();

    private QuerySplittingVisitor() {
        super(ReplaceMode.MUTATE);
    }

    public static class Context {
        private AnalyzedRelation seenRelation;
        private final Set<AnalyzedRelation> seenRelations = Collections.newSetFromMap(new IdentityHashMap<AnalyzedRelation, Boolean>());
        private final Multimap<QualifiedName, Symbol> queries = HashMultimap.create();
        private final List<JoinPair> joinPairs;
        private boolean multiRelation = false;
        private Symbol query;

        public Context(List<JoinPair> joinPairs) {
            this.joinPairs = joinPairs;
        }

        public Symbol query() {
            return query;
        }

        public Multimap<QualifiedName, Symbol> queries() {
            return queries;
        }
    }

    public Context process(Symbol query, List<JoinPair> joinPairs) {
        Context context = new Context(joinPairs);
        context.query = process(query, context);
        if (!context.multiRelation) {
            assert context.seenRelation != null : "context.seenRelation must not be null";
            context.queries.put(context.seenRelation.getQualifiedName(), context.query);
            context.query = null;
        }
        return context;
    }

    @Override
    public Symbol process(Symbol symbol, @Nullable Context context) {
        assert context != null : "context must not be null";
        context.seenRelation = null;
        return super.process(symbol, context);
    }


    @Override
    public Symbol visitField(Field field, Context context) {
        context.seenRelation = field.relation();
        return super.visitField(field, context);
    }

    @Override
    public Symbol visitFunction(Function function, Context context) {
        // ensure we come from bottom up with the last relation
        // context.seenRelations.clear();

        if (AndOperator.NAME.equals(function.info().ident().name())) {
            context.multiRelation = false;
            context.seenRelation = null;
            Symbol left = process(function.arguments().get(0), context);
            function.arguments().set(0, left);
            boolean leftMultiRel = context.multiRelation;
            AnalyzedRelation leftRelation = context.seenRelation;

            context.multiRelation = false;
            context.seenRelation = null;
            Symbol right = process(function.arguments().get(1), context);
            function.arguments().set(1, right);
            boolean rightMultiRel = context.multiRelation;
            AnalyzedRelation rightRelation = context.seenRelation;


            if (leftMultiRel && rightMultiRel) {
                context.multiRelation = true;
                return function;
            } else if (leftMultiRel) {
                context.multiRelation = true;
                if (rightRelation != null) {
                    context.queries.put(rightRelation.getQualifiedName(), right);
                    function.arguments().set(1, Literal.BOOLEAN_TRUE);
                    context.seenRelation = null;
                    return function;
                }
                // pass up, since we are multirel anyways
                return function;
            } else if (rightMultiRel) {
                context.multiRelation = true;
                if (leftRelation != null) {
                    context.queries.put(leftRelation.getQualifiedName(), left);
                    function.arguments().set(0, Literal.BOOLEAN_TRUE);
                    context.seenRelation = null;
                    return function;
                }
                // pass up, since we are multirel anyways
                return function;
            }
            if (leftRelation != rightRelation) {
                if (leftRelation == null) {
                    context.seenRelation = rightRelation;
                    context.multiRelation = false;
                } else if (rightRelation == null) {
                    context.seenRelation = leftRelation;
                    context.multiRelation = false;
                } else {
                    context.queries.put(leftRelation.getQualifiedName(), left);
                    function.arguments().set(0, Literal.BOOLEAN_TRUE);
                    context.seenRelation = rightRelation;
                }
            }
        } else {
            context.seenRelations.clear();
            FieldsVisitor.visitFields(function, f -> context.seenRelations.add(f.relation()));
            context.multiRelation = context.seenRelations.size() > 1;
            if (context.seenRelations.size() == 1) {
                context.seenRelation = Iterables.getOnlyElement(context.seenRelations);
                if (JoinPairs.isOuterRelation(context.seenRelation.getQualifiedName(), context.joinPairs)) {
                    // don't split by marking as multi relation
                    context.multiRelation = true;
                }
            } else if (context.seenRelations.isEmpty()) {
                context.seenRelation = null;
            } else {
                context.multiRelation = true;
                context.seenRelation = null;
            }
        }
        return function;
    }

    @Override
    public Symbol visitMatchPredicate(MatchPredicate matchPredicate, Context context) {
        AnalyzedRelation relation = null;
        for (Field field : matchPredicate.identBoostMap().keySet()) {
            if (relation == null) {
                relation = field.relation();
            } else if (!relation.equals(field.relation())) {
                throw new IllegalArgumentException("Must not use columns from more than 1 relation inside the MATCH predicate");
            }
        }
        context.seenRelation = relation;
        return matchPredicate;
    }
}
