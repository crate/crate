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

import io.crate.core.collections.MapComparator;
import io.crate.data.Input;
import io.crate.expression.operator.Operator;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ArrayType;
import io.crate.types.BooleanType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import io.crate.types.SingleColumnTableType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;

import static com.google.common.base.Preconditions.checkArgument;
import static io.crate.expression.operator.any.AnyOperators.collectionValueToIterable;

public final class AnyOperator extends Operator<Object> {

    public static final String OPERATOR_PREFIX = "any_";

    /*
     * Rewrite `op ANY` to `op` using the actual function names.
     *
     * E.g. `any_=` becomes `op_=`
     */
    public static String nameToNonAny(String functionName) {
        return Operator.PREFIX + functionName.substring(OPERATOR_PREFIX.length());
    }

    private FunctionInfo functionInfo;
    private final IntPredicate cmpIsMatch;

    /**
     * @param cmpIsMatch predicate to test if a comparison (-1, 0, 1) should be considered a match
     */
    AnyOperator(FunctionInfo functionInfo, IntPredicate cmpIsMatch) {
        this.functionInfo = functionInfo;
        this.cmpIsMatch = cmpIsMatch;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @SuppressWarnings("unchecked")
    private Boolean doEvaluate(Object left, Iterable<?> rightIterable) {
        boolean anyNulls = false;
        if (left instanceof Comparable) {
            for (Object elem : rightIterable) {
                if (elem == null) {
                    anyNulls = true;
                    continue;
                }
                assert left.getClass().equals(elem.getClass()) : "class of left must be equal to the class of right";

                if (cmpIsMatch.test(((Comparable) left).compareTo(elem))) {
                    return true;
                }
            }
        } else if (left instanceof Map) {
            for (Object elem : rightIterable) {
                if (cmpIsMatch.test(Objects.compare((Map) left, (Map) elem, MapComparator.getInstance()))) {
                    return true;
                }
            }
        }
        return anyNulls ? null : false;
    }

    @Override
    public Boolean evaluate(Input<Object>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null : "1st argument must not be null";

        Object item = args[0].value();
        Object items = args[1].value();
        if (items == null || item == null) {
            return null;
        }
        return doEvaluate(item, collectionValueToIterable(items));
    }

    public static final class AnyResolver extends BaseFunctionResolver {

        private final String name;
        private final IntPredicate cmpIsMatch;

        AnyResolver(String name, IntPredicate cmpIsMatch) {
            super(FuncParams.builder(
                Param.ANY,
                Param.of(
                    new ArrayType(DataTypes.UNDEFINED),
                    new SetType(DataTypes.UNDEFINED),
                    new SingleColumnTableType(DataTypes.UNDEFINED))
                    .withInnerType(Param.ANY))
                .build());
            this.name = name;
            this.cmpIsMatch = cmpIsMatch;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType<?> innerType = ((CollectionType) dataTypes.get(1)).innerType();
            checkArgument(innerType.equals(dataTypes.get(0)),
                "The inner type of the array/set passed to ANY must match its left expression");
            checkArgument(!innerType.equals(DataTypes.OBJECT),
                "ANY on object arrays is not supported");

            return new AnyOperator(
                new FunctionInfo(new FunctionIdent(name, dataTypes), BooleanType.INSTANCE),
                cmpIsMatch
            );
        }
    }
}
