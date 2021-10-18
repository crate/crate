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

package io.crate.expression.predicate;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;

import io.crate.data.Input;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.any.AnyEqOperator;
import io.crate.expression.operator.any.AnyNeqOperator;
import io.crate.expression.operator.any.AnyRangeOperator;
import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.scalar.conditional.CoalesceFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

public class NotPredicate extends Scalar<Boolean, Boolean> {

    public static final String NAME = "op_not";
    public static final Signature SIGNATURE = Signature.scalar(
        NAME,
        DataTypes.BOOLEAN.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature());

    public static void register(PredicateModule module) {
        module.register(
            SIGNATURE,
            NotPredicate::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    private NotPredicate(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        assert symbol != null : "function must not be null";
        assert symbol.arguments().size() == 1 : "function's number of arguments must be 1";

        Symbol arg = symbol.arguments().get(0);
        if (arg instanceof Input) {
            Object value = ((Input<?>) arg).value();
            if (value == null) {
                // WHERE NOT NULL -> WHERE NULL
                return Literal.of(DataTypes.BOOLEAN, null);
            }
            if (value instanceof Boolean) {
                return Literal.of(!((Boolean) value));
            }
        }
        return symbol;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Boolean>... args) {
        assert args.length == 1 : "number of args must be 1";
        Boolean value = args[0].value();
        return value != null ? !value : null;
    }


    private final SymbolToNotNullRangeQueryArgs INNER_VISITOR = new SymbolToNotNullRangeQueryArgs();

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
                AnyEqOperator.NAME,
                AnyNeqOperator.NAME,
                AnyRangeOperator.Comparison.GT.opName(),
                AnyRangeOperator.Comparison.GTE.opName(),
                AnyRangeOperator.Comparison.LT.opName(),
                AnyRangeOperator.Comparison.LTE.opName(),
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
    public Query toQuery(Function input, LuceneQueryBuilder.Context context) {
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
                return IsNullPredicate.refExistsQuery(ref, context);
            }
        }

        Query innerQuery = arg.accept(context.visitor(), context);
        Query notX = Queries.not(innerQuery);

        // not x =  not x & x is not null
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        builder.add(notX, BooleanClause.Occur.MUST);
        SymbolToNotNullContext ctx = new SymbolToNotNullContext();
        arg.accept(INNER_VISITOR, ctx);
        for (Reference reference : ctx.references()) {
            if (reference.isNullable()) {
                builder.add(IsNullPredicate.refExistsQuery(reference, context), BooleanClause.Occur.MUST);
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
