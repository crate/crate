/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.operator;

import io.crate.common.collections.MapComparator;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntPredicate;

public final class CmpOperator extends Operator<Object> {

    private final FunctionInfo info;
    private final IntPredicate isMatch;

    public CmpOperator(FunctionInfo info, IntPredicate cmpResultIsMatch) {
        this.info = info;
        this.isMatch = cmpResultIsMatch;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<Object>... args) {
        assert args != null : "args must not be null";
        assert args.length == 2 : "number of args must be 2";
        assert args[0] != null && args[1] != null : "1st and 2nd argument must not be null";

        Object left = args[0].value();
        Object right = args[1].value();
        if (left == null || right == null) {
            return null;
        }

        assert (left.getClass().equals(right.getClass())) : "left and right must have the same type for comparison";

        if (left instanceof Comparable) {
            return isMatch.test(((Comparable) left).compareTo(right));
        } else if (left instanceof Map) {
            return isMatch.test(Objects.compare((Map) left, (Map) right, MapComparator.getInstance()));
        } else {
            return null;
        }
    }

    protected static class CmpResolver extends BaseFunctionResolver {

        private static final Param PRIMITIVE_TYPES = Param.of(DataTypes.PRIMITIVE_TYPES);

        private final String name;
        private final IntPredicate isMatch;

        CmpResolver(String name, IntPredicate isMatch) {
            this(name, FuncParams.builder(PRIMITIVE_TYPES, PRIMITIVE_TYPES).build(), isMatch);
        }

        CmpResolver(String name, FuncParams funcParams, IntPredicate isMatch) {
            super(funcParams);
            this.name = name;
            this.isMatch = isMatch;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            FunctionInfo info = createInfo(name, dataTypes);
            return new CmpOperator(info, isMatch);
        }


        protected static FunctionInfo createInfo(String name, List<DataType> dataTypes) {
            return new FunctionInfo(new FunctionIdent(name, dataTypes), DataTypes.BOOLEAN);
        }
    }
}
