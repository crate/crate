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
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;

import io.crate.data.Input;
import io.crate.expression.operator.AndOperator;
import io.crate.expression.operator.EqOperator;
import io.crate.expression.operator.OrOperator;
import io.crate.expression.operator.any.AnyRangeOperator;
import io.crate.expression.scalar.Ignore3vlFunction;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionReferenceResolver;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

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


    private final SkipThreeValuedLogicVisitor SKIP_3VL_VISITOR = new SkipThreeValuedLogicVisitor();

    private static class SkipThreeValuedLogicContext {

        private final HashSet<Reference> references = new HashSet<>();
        boolean skip3ValuedLogic = false;
        boolean containsFunction = false;

        void add(Reference symbol) {
            references.add(symbol);
        }

        Set<Reference> references() {
            return references;
        }

        boolean skip() {
            return skip3ValuedLogic || false == containsFunction;
        }
    }

    /**
     * Three valued logic systems are defined in logic as systems in which there are 3 truth values: true,
     * false and an indeterminate third value (in our case null is the third value).
     * <p>
     * This is a set of cases for which inputs evaluating to null needs can be explicitly excluded
     * in the boolean queries
     */
    private static class SkipThreeValuedLogicVisitor extends SymbolVisitor<SkipThreeValuedLogicContext, Void> {

        private final Set<String> IGNORE_3VL_FUNCTIONS = Set.of(
            MatchPredicate.NAME,
            Ignore3vlFunction.NAME
        );

        private final Set<String> CAST_FUNCTIONS = Set.of(
            ImplicitCastFunction.NAME,
            ExplicitCastFunction.NAME
        );

        private final Set<String> COMPARISON_OPERATORS = Set.of(
            EqOperator.NAME,
            AndOperator.NAME,
            OrOperator.NAME,
            AnyRangeOperator.Comparison.GT.opName(),
            AnyRangeOperator.Comparison.GTE.opName(),
            AnyRangeOperator.Comparison.LT.opName(),
            AnyRangeOperator.Comparison.LTE.opName()
        );

        @Override
        public Void visitReference(Reference symbol, SkipThreeValuedLogicContext context) {
            context.add(symbol);
            return null;
        }

        @Override
        public Void visitFunction(Function function, SkipThreeValuedLogicContext context) {
            String functionName = function.name();
            if (IGNORE_3VL_FUNCTIONS.contains(functionName)) {
                context.skip3ValuedLogic = true;
                return null;
            } else if (CAST_FUNCTIONS.contains(functionName)) {
                var arg = function.arguments().get(0);
                if (arg instanceof Reference ref) {
                    DataType<?> dataType = ref.valueType();
                    if (ref.valueType() instanceof ObjectType) {
                        // Skip 3vl logic for objects to make sure empty objects don't match
                        context.skip3ValuedLogic = true;
                    }
                }
            } else if (COMPARISON_OPERATORS.contains(functionName)) {
                context.containsFunction = true;
                var a = function.arguments().get(0);
                var b = function.arguments().get(1);
                // In the case of a comparison operator when the value is not null such as
                // x = `foo` 3vl can be skipped because if x is null the comparison is false.
                if (a instanceof Reference && b instanceof Literal<?> ||
                   a instanceof Literal<?> && b instanceof Reference) {
                    context.skip3ValuedLogic = true;
                }
            } else {
                context.containsFunction = true;
            }
            for (Symbol arg : function.arguments()) {
                arg.accept(this, context);
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
                return IsNullPredicate.refExistsQuery(ref, context, true);
            }
        }

        Query innerQuery = arg.accept(context.visitor(), context);
        Query notX = Queries.not(innerQuery);

        /* When the three-valued-logic can be skipped we ignore null values.
         * In this case we can add an optimization ignoring all matches where the field is not present.
         * Three-valued-logic can be skipped under the following conditions:
         * - No function is involved `NOT x`
         * - Simple equality from a reference to a value `NOT x = 1`
         * - ignore3vl function `ignore3vl( ... )`
         */
        var skip3vl = new SkipThreeValuedLogicContext();
        arg.accept(SKIP_3VL_VISITOR, skip3vl);

        if (skip3vl.skip()) {
            BooleanQuery.Builder builder = new BooleanQuery.Builder();
            builder.add(notX, BooleanClause.Occur.MUST);
            for (Reference reference : skip3vl.references()) {
                if (reference.isNullable()) {
                    var refExistsQuery = IsNullPredicate.refExistsQuery(reference, context, false);
                    if (refExistsQuery != null) {
                        builder.add(refExistsQuery, BooleanClause.Occur.MUST);
                    }
                }
            }
            return builder.build();
        }

        return new BooleanQuery.Builder()
            .add(notX, Occur.MUST)
            .add(LuceneQueryBuilder.genericFunctionFilter(input, context), Occur.FILTER)
            .build();
    }
}
