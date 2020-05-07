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
import io.crate.expression.operator.Operator;
import io.crate.expression.operator.TriPredicate;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
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

public class AnyLikeOperator extends Operator<Object> {

    public static FunctionResolver resolverFor(String name,
                                               TriPredicate<String, String, Integer> matcher,
                                               int patternMatchingFlags) {
        return new AnyLikeResolver(name, matcher, patternMatchingFlags);
    }

    private final FunctionInfo info;
    private final TriPredicate<String, String, Integer> matcher;
    private final int patternMatchingFlags;

    private AnyLikeOperator(FunctionInfo info,
                            TriPredicate<String, String, Integer> matcher,
                            int patternMatchingFlags) {
        this.info = info;
        this.matcher = matcher;
        this.patternMatchingFlags = patternMatchingFlags;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    private Boolean doEvaluate(Object left, Iterable<?> rightIterable) {
        String pattern = (String) left;
        boolean hasNull = false;
        for (Object elem : rightIterable) {
            if (elem == null) {
                hasNull = true;
                continue;
            }
            assert elem instanceof String : "elem must be a String";
            String elemValue = (String) elem;
            if (matcher.test(elemValue, pattern, patternMatchingFlags)) {
                return true;
            }
        }
        return hasNull ? null : false;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<Object>... args) {
        Object value = args[0].value();
        Object collectionReference = args[1].value();

        if (collectionReference == null || value == null) {
            return null;
        }
        return doEvaluate(value, collectionValueToIterable(collectionReference));
    }

    private static class AnyLikeResolver extends BaseFunctionResolver {
        private final String name;
        private final TriPredicate<String, String, Integer> matches;
        private final int patternMatchingFlags;

        AnyLikeResolver(String name,
                        TriPredicate<String, String, Integer> matches,
                        int patternMatchingFlags) {
            super(FuncParams.builder(
                Param.ANY,
                Param.of(
                    new ArrayType<>(DataTypes.UNDEFINED))
                    .withInnerType(Param.ANY))
                      .build());
            this.name = name;
            this.matches = matches;
            this.patternMatchingFlags = patternMatchingFlags;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType<?> innerType = ((ArrayType) dataTypes.get(1)).innerType();
            checkArgument(innerType.equals(dataTypes.get(0)),
                          "The inner type of the array/set passed to ANY must match its left expression");
            checkArgument(innerType.id() != ObjectType.ID,
                          "ANY on object arrays is not supported");

            return new AnyLikeOperator(
                new FunctionInfo(new FunctionIdent(name, dataTypes), DataTypes.BOOLEAN),
                matches,
                patternMatchingFlags
            );
        }
    }
}
