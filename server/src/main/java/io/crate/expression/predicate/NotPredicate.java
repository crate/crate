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

import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;

import io.crate.data.Input;
import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataTypes;

public class NotPredicate extends Scalar<Boolean, Boolean> {

    public static final String NAME = "op_not";
    public static final Signature SIGNATURE = Signature.scalar(
        NAME,
        DataTypes.BOOLEAN.getTypeSignature(),
        DataTypes.BOOLEAN.getTypeSignature())
        .withFeature(Feature.NULLABLE);

    public static void register(PredicateModule module) {
        module.register(
            SIGNATURE,
            NotPredicate::new
        );
    }

    private NotPredicate(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
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
            if (value instanceof Boolean b) {
                return Literal.of(!b);
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


    private final NullabilityVisitor INNER_VISITOR = new NullabilityVisitor();

    private static class NullabilityContext {
        private final HashSet<Reference> nullableReferences = new HashSet<>();
        private boolean isNullable = true;
        private boolean enforceThreeValuedLogic = false;

        void collectNullableReferences(Reference symbol) {
            nullableReferences.add(symbol);
        }

        Set<Reference> nullableReferences() {
            return nullableReferences;
        }

        boolean enforceThreeValuedLogic() {
            return enforceThreeValuedLogic;
        }
    }

    private static class NullabilityVisitor extends SymbolVisitor<NullabilityContext, Void> {

        private final Set<String> CAST_FUNCTIONS = Set.of(ImplicitCastFunction.NAME, ExplicitCastFunction.NAME);

        @Override
        public Void visitReference(Reference symbol, NullabilityContext context) {
            // if ref is nullable and all its parents are nullable
            if (symbol.isNullable() && context.isNullable) {
                context.collectNullableReferences(symbol);
            }
            return null;
        }

        @Override
        public Void visitFunction(Function function, NullabilityContext context) {
            String functionName = function.name();
            if (CAST_FUNCTIONS.contains(functionName)) {
                // Cast functions should be ignored except for the case where the incoming
                // datatype is an object. There we need to exclude null values to not match
                // empty objects on the query
                var a = function.arguments().get(0);
                var b = function.arguments().get(1);
                if (a instanceof Reference ref && b instanceof Literal<?>) {
                    if (ref.valueType().id() == DataTypes.UNTYPED_OBJECT.id()) {
                        return null;
                    }
                }
            } else if (Ignore3vlFunction.NAME.equals(functionName)) {
                context.isNullable = false;
                return null;
            } else {
                var signature = function.signature();
                if (signature.hasFeature(Feature.NON_NULLABLE)) {
                    context.isNullable = false;
                } else if (!signature.hasFeature(Feature.NULLABLE)) {
                    // default case
                    context.enforceThreeValuedLogic = true;
                    return null;
                }
            }
            // saves and restores isNullable of the current context
            // such that any non-nullables observed from the left arg is not transferred to the right arg.
            boolean isNullable = context.isNullable;
            for (Symbol arg : function.arguments()) {
                arg.accept(this, context);
                context.isNullable = isNullable;
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
        if (arg instanceof Function innerFunction && innerFunction.name().equals(IsNullPredicate.NAME)) {
            if (innerFunction.arguments().size() == 1 && innerFunction.arguments().get(0) instanceof Reference ref) {
                // Ignored objects have no field names in the index, need function filter fallback
                if (ref.columnPolicy() == ColumnPolicy.IGNORED) {
                    return null;
                }
                if (!ref.isNullable()) {
                    return new MatchAllDocsQuery();
                }
                return IsNullPredicate.refExistsQuery(ref, context, true);
            }
        }

        Query innerQuery = arg.accept(context.visitor(), context);
        Query notX = Queries.not(innerQuery);

        NullabilityContext ctx = new NullabilityContext();
        arg.accept(INNER_VISITOR, ctx);

        if (ctx.enforceThreeValuedLogic()) {
            // we require strict 3vl logic, therefore we need to add the function as generic function filter
            // which is less efficient
            return new BooleanQuery.Builder()
                .add(notX, Occur.MUST)
                .add(LuceneQueryBuilder.genericFunctionFilter(input, context), Occur.FILTER)
                .build();
        } else {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(notX, BooleanClause.Occur.MUST);
            for (Reference nullableRef : ctx.nullableReferences()) {
                // we can optimize with a field exist query and filter out all null values which will reduce the
                // result set of the query
                var refExistsQuery = IsNullPredicate.refExistsQuery(nullableRef, context, false);
                if (refExistsQuery != null) {
                    builder.add(refExistsQuery, BooleanClause.Occur.MUST);
                }
            }
            return builder.build();
        }
    }
}
