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

package io.crate.operation.scalar.arithmetic;

import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;


public abstract class PowerFunction extends Scalar<Number, Number> {

    public static final String NAME = "power";

    private final FunctionInfo info;

    PowerFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    private static class DoublePowerFunction extends PowerFunction {

        DoublePowerFunction(FunctionInfo info) {
            super(info);
        }

        @Override
        public Number evaluate(Input[] args) {
            assert args.length == 2;
            Object arg0Value = args[0].value();
            Object arg1Value = args[1].value();

            if (arg0Value == null) {
                return null;
            }
            if (arg1Value == null) {
                return null;
            }
            return Math.pow(((Number) arg0Value).doubleValue(), ((Number) arg1Value).doubleValue());
        }
    }

    private static class Resolver implements DynamicFunctionResolver {
        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            if (dataTypes.size() != 2) {
                throw new IllegalArgumentException(
                    "The number of arguments passed to power(...) must be 2. Got " + dataTypes.size());
            }

            for (DataType dataType : dataTypes) {
                if (!DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(dataType)) {
                    throw new IllegalArgumentException("Received unsupported argument type " + dataType);
                }
            }
            FunctionInfo powerFunctionInfo =
                new FunctionInfo(new FunctionIdent(NAME, dataTypes), DataTypes.DOUBLE);
            return new DoublePowerFunction(powerFunctionInfo);
        }
    }
}
