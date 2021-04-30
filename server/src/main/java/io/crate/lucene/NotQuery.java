/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.lucene;

import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.any.AnyOperators;
import io.crate.expression.predicate.IsNullPredicate;
import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.metadata.Reference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.query.ExistsQueryBuilder;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

final class NotQuery implements FunctionToQuery {

    private final SymbolToNotNullRangeQueryArgs INNER_VISITOR = new SymbolToNotNullRangeQueryArgs();
    private final LuceneQueryBuilder.Visitor visitor;

    public NotQuery(LuceneQueryBuilder.Visitor visitor) {
        this.visitor = visitor;
    }

    private static class SymbolToNotNullContext {
        private final HashSet<Reference> references = new HashSet<>();
        boolean hasStrictThreeValuedLogicFunction = false;

        void add(Reference symbol) {
            references.add(symbol);
        }

        Set<Reference> references() {
            return references;
        }
    }

    private static class SymbolToNotNullRangeQueryArgs extends SymbolVisitor<SymbolToNotNullContext, Void> {

        /**
         * Three valued logic systems are defined in logic as systems in which there are 3 truth values: true,
         * false and an indeterminate third value (in our case null is the third value).
         * <p>
         * This is a set of functions for which inputs evaluating to null needs to be explicitly included or
         * excluded (in the case of 'not ...') in the boolean queries
         */
        private final Set<String> STRICT_3VL_FUNCTIONS =
            Set.of(
                AnyOperators.Type.EQ.opName(),
                AnyOperators.Type.NEQ.opName(),
                AnyOperators.Type.GTE.opName(),
                AnyOperators.Type.GT.opName(),
                AnyOperators.Type.LTE.opName(),
                AnyOperators.Type.LT.opName(),
                LikeOperators.ANY_LIKE,
                LikeOperators.ANY_NOT_LIKE,
                CoalesceFunction.NAME
            );

        @Override
        public Void visitReference(Reference symbol, SymbolToNotNullContext context) {
            context.add(symbol);
            return null;
        }

        @Override
        public Void visitFunction(Function function, SymbolToNotNullContext context) {
            String functionName = function.name();
            if (Ignore3vlFunction.NAME.equals(functionName)) {
                return null;
            }
            if (!STRICT_3VL_FUNCTIONS.contains(functionName)) {
                for (Symbol arg : function.arguments()) {
                    arg.accept(this, context);
                }
            } else {
                context.hasStrictThreeValuedLogicFunction = true;
            }
            return null;
        }
    }


    @Override
    public Query apply(Function input, LuceneQueryBuilder.Context context) {
        assert input != null : "function must not be null";
        assert input.arguments().size() == 1 : "function's number of arguments must be 1";
        /**
         * not null -> null     -> no match
         * not true -> false    -> no match
         * not false -> true    -> match
         */

        // handles not true / not false
        Symbol arg = input.arguments().get(0);

        // Optimize `NOT (<ref> IS NULL)`
        if (arg instanceof Function && ((Function) arg).name().equals(IsNullPredicate.NAME)) {
            Function innerFunction = (Function) arg;
            if (innerFunction.arguments().size() == 1 && innerFunction.arguments().get(0) instanceof Reference) {
                Reference ref = (Reference) innerFunction.arguments().get(0);
                // Ignored objects have no field names in the index, need function filter fallback
                if (ref.columnPolicy() == ColumnPolicy.IGNORED) {
                    return null;
                }
                return ExistsQueryBuilder.newFilter(context.queryShardContext, ref.column().fqn());
            }
        }

        Query innerQuery = arg.accept(visitor, context);
        Query notX = Queries.not(innerQuery);

        // not x =  not x & x is not null
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(notX, BooleanClause.Occur.MUST);
        SymbolToNotNullContext ctx = new SymbolToNotNullContext();
        arg.accept(INNER_VISITOR, ctx);
        for (Reference reference : ctx.references()) {
            String columnName = reference.column().fqn();
            if (reference.isNullable()) {
                builder.add(ExistsQueryBuilder.newFilter(context.queryShardContext, columnName), BooleanClause.Occur.MUST);
            }
        }
        if (ctx.hasStrictThreeValuedLogicFunction) {
            Function isNullFunction = new Function(
                IsNullPredicate.SIGNATURE,
                Collections.singletonList(arg),
                DataTypes.BOOLEAN
            );
            builder.add(
                Queries.not(LuceneQueryBuilder.genericFunctionFilter(isNullFunction, context)),
                BooleanClause.Occur.MUST
            );
        }
        return builder.build();
    }
}
