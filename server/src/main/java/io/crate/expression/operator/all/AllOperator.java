/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.operator.all;

import java.util.Collection;
import java.util.List;

import org.apache.lucene.search.Query;

import io.crate.data.Input;
import io.crate.expression.operator.LikeOperators;
import io.crate.expression.operator.Operator;
import io.crate.expression.scalar.ArrayUnnestFunction;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.FunctionProvider;
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

public abstract sealed class AllOperator<T> extends Operator<T> permits AllEqOperator, AllLikeOperator, AllNeqOperator, AllNotLikeOperator, AllRangeOperator {

    public static final String OPERATOR_PREFIX = "_all_";
    public static final List<String> OPERATOR_NAMES = List.of(
        AllEqOperator.NAME,
        AllNeqOperator.NAME,
        AllRangeOperator.Comparison.GT.opName(),
        AllRangeOperator.Comparison.GTE.opName(),
        AllRangeOperator.Comparison.LT.opName(),
        AllRangeOperator.Comparison.LTE.opName(),
        LikeOperators.ALL_LIKE,
        LikeOperators.ALL_ILIKE,
        LikeOperators.ALL_NOT_LIKE,
        LikeOperators.ALL_NOT_ILIKE
    );

    protected final DataType<Object> leftType;

    public static void register(Functions.Builder builder) {
        reg(builder, AllEqOperator.NAME, AllEqOperator::new);
        reg(builder, AllNeqOperator.NAME, AllNeqOperator::new);
        regRange(builder, AllRangeOperator.Comparison.GT);
        regRange(builder, AllRangeOperator.Comparison.GTE);
        regRange(builder, AllRangeOperator.Comparison.LT);
        regRange(builder, AllRangeOperator.Comparison.LTE);
    }

    private static void regRange(Functions.Builder builder, AllRangeOperator.Comparison comparison) {
        reg(builder, comparison.opName(), (sig, boundSig) -> new AllRangeOperator(sig, boundSig, comparison));
    }

    private static void reg(Functions.Builder builder, String name, FunctionProvider.FunctionFactory operatorFactory) {
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
    public AllOperator(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
        this.leftType = (DataType<Object>) boundSignature.argTypes().get(0);
    }

    abstract boolean matches(T probe, T candidate);

    protected abstract Query refMatchesAllArrayLiteral(Function all, Reference probe, Literal<?> literal, LuceneQueryBuilder.Context context);

    protected abstract Query literalMatchesAllArrayRef(Function all, Literal<?> probe, Reference candidates, LuceneQueryBuilder.Context context);

    protected void validateRightArg(T arg) {
    }

    @SuppressWarnings("unchecked")
    @SafeVarargs
    @Override
    public final Boolean evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<T>... args) {
        T leftValue = args[0].value();
        Collection<T> rightValues = (Collection<T>) args[1].value();
        if (leftValue == null || rightValues == null) {
            return null;
        }
        boolean anyNulls = false;
        int matches = 0;
        for (T rightValue : rightValues) {
            if (rightValue == null) {
                anyNulls = true;
                matches++;
            } else {
                validateRightArg(rightValue);
                if (matches(leftValue, rightValue)) {
                    matches++;
                }
            }
        }
        if (matches == rightValues.size()) {
            return anyNulls ? null : true;
        } else {
            return false;
        }
    }

    @Override
    public Query toQuery(Function function, LuceneQueryBuilder.Context context) {
        List<Symbol> args = function.arguments();
        Symbol probe = args.get(0);
        Symbol candidates = args.get(1);
        while (candidates instanceof Function fn && fn.signature().equals(ArrayUnnestFunction.SIGNATURE)) {
            candidates = fn.arguments().getFirst();
        }
        if (probe instanceof Literal<?> literal && candidates instanceof Reference ref) {
            return literalMatchesAllArrayRef(function, literal, ref, context);
        } else if (probe instanceof Reference ref && candidates instanceof Literal<?> literal) {
            return refMatchesAllArrayLiteral(function, ref, literal, context);
        } else {
            return null;
        }
    }
}
