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

package io.crate.execution.expression.scalar.arithmetic;

import io.crate.analyze.symbol.FuncArg;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Locale;
import java.util.Map;


abstract class SingleArgumentArithmeticFunction extends Scalar<Number, Number> {

    protected FunctionInfo info;

    SingleArgumentArithmeticFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    static FunctionInfo generateFloatFunctionInfo(String name, List<DataType> dataTypes) {
        return new FunctionInfo(
            new FunctionIdent(name, dataTypes),
            DataTypes.INTEGER,
            FunctionInfo.Type.SCALAR,
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT
        );
    }

    static FunctionInfo generateDoubleFunctionInfo(String name, List<DataType> dataTypes) {
        return new FunctionInfo(
            new FunctionIdent(name, dataTypes),
            DataTypes.LONG,
            FunctionInfo.Type.SCALAR,
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT
        );
    }

    static class Resolver implements FunctionResolver {

        private final String name;
        private final Map<DataType, SingleArgumentArithmeticFunction> registry;

        public Resolver(String name, Map<DataType, SingleArgumentArithmeticFunction> registry) {
            this.name = name;
            this.registry = registry;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            assert dataTypes.size() == 1 : name + " function excepts exactly one argument type";
            SingleArgumentArithmeticFunction impl = registry.get(dataTypes.get(0));
            if (impl == null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Could not resolve function '%s' with argument type '%s'", name, dataTypes.get(0))
                );
            }
            return impl;
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> arguments) {
            return FuncParams.SINGLE_NUMERIC.match(arguments);
        }
    }
}
