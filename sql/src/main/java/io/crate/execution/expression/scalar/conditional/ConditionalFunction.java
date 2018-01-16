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

package io.crate.execution.expression.scalar.conditional;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.Param;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

abstract class ConditionalFunction extends Scalar<Object, Object> {

    private final FunctionInfo info;

    ConditionalFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @VisibleForTesting
    static FunctionInfo createInfo(String name, List<DataType> dataTypes) {
        Set<DataType> types = new HashSet<>(dataTypes);
        Preconditions.checkArgument(types.size() > 0, "%s function requires at least one argument", name);
        //  NULL is allowed additionally to any other dataType (NULL == UNDEFINED)
        Preconditions.checkArgument(types.size() == 1 || (types.size() == 2 && types.contains(DataTypes.UNDEFINED)),
            "all arguments for %s function must have the same data type", name);
        return new FunctionInfo(new FunctionIdent(name, dataTypes), DataTypes.tryFindNotNullType(dataTypes));
    }

    static class ConditionalFunctionResolver extends BaseFunctionResolver {

        private final String name;
        private final Function<FunctionInfo, FunctionImplementation> fn;

        ConditionalFunctionResolver(String name, Function<FunctionInfo, FunctionImplementation> fn) {
            super(FuncParams.builder().withVarArgs(Param.ANY).build());
            this.name = name;
            this.fn = fn;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            return fn.apply(createInfo(name, dataTypes));
        }
    }
}
