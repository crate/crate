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
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import io.crate.types.SingleColumnTableType;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.function.BiPredicate;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static io.crate.expression.operator.any.AnyOperators.collectionValueToIterable;

public final class AnyLikeOperator extends Operator<Object> {

    public static final String LIKE = AnyOperator.OPERATOR_PREFIX + "like";
    public static final String NOT_LIKE = AnyOperator.OPERATOR_PREFIX + "not_like";
    private final FunctionInfo info;
    private final BiPredicate<String, String> matches;

    public static void register(OperatorModule operatorModule) {
        operatorModule.registerDynamicOperatorFunction(
            LIKE,
            new AnyLikeResolver(LIKE, AnyLikeOperator::matches));
        operatorModule.registerDynamicOperatorFunction(
            NOT_LIKE,
            new AnyLikeResolver(NOT_LIKE, ((BiPredicate<String, String>) AnyLikeOperator::matches).negate()));
    }

    private static boolean matches(String expr, String pattern) {
        return Pattern.matches(LikeOperator.patternToRegex(pattern, LikeOperator.DEFAULT_ESCAPE, true), expr);
    }

    AnyLikeOperator(FunctionInfo info, BiPredicate<String, String> matches) {
        this.info = info;
        this.matches = matches;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    private Boolean doEvaluate(Object left, Iterable<?> rightIterable) {
        BytesRef rightBytesRef = (BytesRef) left;
        String pattern = rightBytesRef.utf8ToString();

        boolean hasNull = false;
        for (Object elem : rightIterable) {
            if (elem == null) {
                hasNull = true;
                continue;
            }
            assert elem instanceof BytesRef || elem instanceof String : "elem must be BytesRef or String";

            String elemValue = elem instanceof BytesRef ? ((BytesRef) elem).utf8ToString() : (String) elem;
            if (matches.test(elemValue, pattern)) {
                return true;
            }
        }
        return hasNull ? null : false;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        Object value = args[0].value();
        Object collectionReference = args[1].value();

        if (collectionReference == null || value == null) {
            return null;
        }
        return doEvaluate(value, collectionValueToIterable(collectionReference));
    }

    private static class AnyLikeResolver extends BaseFunctionResolver {

        private final String name;
        private final BiPredicate<String, String> matches;

        AnyLikeResolver(String name, BiPredicate<String, String> matches) {
            super(FuncParams.builder(
                Param.ANY,
                Param.of(
                    new ArrayType(DataTypes.UNDEFINED),
                    new SetType(DataTypes.UNDEFINED),
                    new SingleColumnTableType(DataTypes.UNDEFINED))
                    .withInnerType(Param.ANY))
                .build());
            this.name = name;
            this.matches = matches;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType<?> innerType = ((CollectionType) dataTypes.get(1)).innerType();
            checkArgument(innerType.equals(dataTypes.get(0)),
                "The inner type of the array/set passed to ANY must match its left expression");
            checkArgument(!innerType.equals(DataTypes.OBJECT),
                "ANY on object arrays is not supported");

            return new AnyLikeOperator(
                new FunctionInfo(new FunctionIdent(name, dataTypes), DataTypes.BOOLEAN),
                matches
            );
        }
    }
}
