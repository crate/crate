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

package io.crate.expression.operator.any;

import io.crate.data.Input;
import io.crate.expression.operator.LikeOperator;
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.OperatorModule;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.crate.expression.operator.any.AnyOperators.collectionValueToIterable;

public final class AnyLikeOperator extends Operator<String> {

    public static final String LIKE = AnyOperator.OPERATOR_PREFIX + "like";
    public static final String NOT_LIKE = AnyOperator.OPERATOR_PREFIX + "not_like";

    private FunctionInfo info;
    private final TriPredicate<String, String, Boolean> matches;

    public static void register(OperatorModule operatorModule) {
        operatorModule.registerDynamicOperatorFunction(
            LIKE,
            new AnyLikeResolver(LIKE, LikeOperator::matches));
        operatorModule.registerDynamicOperatorFunction(
            NOT_LIKE,
            new AnyLikeResolver(NOT_LIKE, TriPredicate.negate(LikeOperator::matches)));
    }

    private AnyLikeOperator(FunctionInfo info, TriPredicate<String, String, Boolean> matches) {
        this.info = info;
        this.matches = matches;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    private Boolean doEvaluate(String pattern, Iterable<?> rightIterable, boolean ignoreCase) {
        boolean hasNull = false;
        for (Object elem : rightIterable) {
            if (elem == null) {
                hasNull = true;
                continue;
            }
            assert elem instanceof String : "elem must be a String";
            String elemValue = (String) elem;
            if (matches.test(elemValue, pattern, ignoreCase)) {
                return true;
            }
        }
        return hasNull ? null : false;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<String>... args) {
        String value = args[0].value();
        Object collectionReference = args[1].value();
        boolean ignoreCase = (Boolean) ((Object) args[2].value());
        if (collectionReference == null || value == null) {
            return null;
        }
        return doEvaluate(value, collectionValueToIterable(collectionReference), ignoreCase);
    }

    @FunctionalInterface
    private interface TriPredicate<A, B, C> {
        boolean test(A a, B b, C c);

        static <A, B, C> TriPredicate<A, B, C> negate(TriPredicate<A, B, C> pred) {
            return pred.negate();
        }

        default TriPredicate<A, B, C> negate() {
            return (A a, B b, C c) -> false == test(a, b, c);
        }
    }

    private static class AnyLikeResolver extends BaseFunctionResolver {
        private final String name;
        private final TriPredicate<String, String, Boolean> matches;

        AnyLikeResolver(String name, TriPredicate<String, String, Boolean> matches) {
            super(FuncParams.builder(
                Param.ANY,
                Param.of(new ArrayType<>(DataTypes.UNDEFINED)).withInnerType(Param.ANY),
                Param.BOOLEAN).build());
            this.name = name;
            this.matches = matches;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType<?> innerType = ((ArrayType) dataTypes.get(1)).innerType();
            checkArgument(innerType.equals(dataTypes.get(0)),
                "The inner type of the array/set passed to ANY must match its left expression");
            checkArgument(innerType.id() != ObjectType.ID,
                "ANY on object arrays is not supported");
            return new AnyLikeOperator(new FunctionInfo(new FunctionIdent(name, dataTypes), DataTypes.BOOLEAN), matches);
        }
    }
}
