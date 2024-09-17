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

package io.crate.expression.operator.any;

import java.util.List;
import java.util.Objects;
import java.util.stream.StreamSupport;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.jetbrains.annotations.NotNull;

import io.crate.data.Input;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.any.AnyRangeOperator.Comparison;
import io.crate.expression.scalar.ArrayUnnestFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder.Context;
import io.crate.metadata.FunctionProvider.FunctionFactory;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.DataType;
import io.crate.types.TypeSignature;


/**
 * Abstract base class for implementations of {@code <probe> <op> ANY(<candidates>)}
 **/
public abstract sealed class AnyOperator<T> extends Operator<T>
    permits AnyEqOperator, AnyNeqOperator, AnyRangeOperator, AnyLikeOperator, AnyNotLikeOperator {

    public static final String OPERATOR_PREFIX = "any_";
    public static final List<String> OPERATOR_NAMES = List.of(
        AnyEqOperator.NAME,
        AnyNeqOperator.NAME,
        AnyRangeOperator.Comparison.GT.opName(),
        AnyRangeOperator.Comparison.GTE.opName(),
        AnyRangeOperator.Comparison.LT.opName(),
        AnyRangeOperator.Comparison.LTE.opName(),
        LikeOperators.ANY_LIKE,
        LikeOperators.ANY_ILIKE,
        LikeOperators.ANY_NOT_LIKE,
        LikeOperators.ANY_NOT_ILIKE
    );

    protected final DataType<T> leftType;

    public static void register(Functions.Builder builder) {
        reg(builder, AnyEqOperator.NAME, AnyEqOperator::new);
        reg(builder, AnyNeqOperator.NAME, AnyNeqOperator::new);
        regRange(builder, AnyRangeOperator.Comparison.GT);
        regRange(builder, AnyRangeOperator.Comparison.GTE);
        regRange(builder, AnyRangeOperator.Comparison.LT);
        regRange(builder, AnyRangeOperator.Comparison.LTE);
    }

    private static void regRange(Functions.Builder builder, Comparison comparison) {
        reg(builder, comparison.opName(), (sig, boundSig) -> new AnyRangeOperator(sig, boundSig, comparison));
    }

    private static void reg(Functions.Builder builder, String name, FunctionFactory operatorFactory) {
        builder.add(
            Signature.builder(name, FunctionType.SCALAR)
                .argumentTypes(TypeSignature.parse("E"),
                    TypeSignature.parse("array(E)"))
                .returnType(Operator.RETURN_TYPE.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .typeVariableConstraints(TypeVariableConstraint.typeVariable("E"))
                .build(),
            operatorFactory
        );
    }

    @SuppressWarnings("unchecked")
    AnyOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        this.leftType = (DataType<T>) boundSignature.argTypes().get(0);
    }

    abstract boolean matches(T probe, T candidate);

    protected abstract Query refMatchesAnyArrayLiteral(Function any, Reference probe, @NotNull List<?> nonNullValues, Context context);

    protected abstract Query literalMatchesAnyArrayRef(Function any, Literal<?> probe, Reference candidates, Context context);

    protected void validateRightArg(T arg) {}

    @SuppressWarnings("unchecked")
    @Override
    @SafeVarargs
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T> ... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null : "1st argument must not be null";

        T item = args[0].value();
        T items = args[1].value();
        if (items == null || item == null) {
            return null;
        }
        boolean anyNulls = false;
        for (T rightValue : (Iterable<T>) items) {
            if (rightValue == null) {
                anyNulls = true;
                continue;
            }
            validateRightArg(rightValue);
            if (matches(item, rightValue)) {
                return true;
            }
        }
        return anyNulls ? null : false;
    }

    @Override
    public Query toQuery(Function function, Context context) {
        List<Symbol> args = function.arguments();
        Symbol probe = args.get(0);
        Symbol candidates = args.get(1);
        while (candidates instanceof Function fn && fn.signature().equals(ArrayUnnestFunction.SIGNATURE)) {
            candidates = fn.arguments().get(0);
        }
        if (probe instanceof Literal<?> literal && candidates instanceof Reference ref) {
            return literalMatchesAnyArrayRef(function, literal, ref, context);
        } else if (probe instanceof Reference ref && candidates instanceof Literal<?> literal) {
            var nonNullValues = StreamSupport
                .stream(((Iterable<?>) literal.value()).spliterator(), false)
                .filter(Objects::nonNull).toList();
            if (nonNullValues.isEmpty()) {
                return new MatchNoDocsQuery("Cannot match unless there is at least one non-null candidate");
            }
            return refMatchesAnyArrayLiteral(function, ref, nonNullValues, context);
        } else {
            return null;
        }
    }
}
