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

package io.crate.operation.scalar.arithmetic;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.data.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public abstract class CeilFunction extends SingleArgumentArithmeticFunction {

    public static final String NAME = "ceil";

    CeilFunction(FunctionInfo info) {
        super(info);
    }

    public static void register(ScalarFunctionModule module) {
        Map<DataType, SingleArgumentArithmeticFunction> functionMap =
            ImmutableMap.<DataType, SingleArgumentArithmeticFunction>builder()
            .put(DataTypes.FLOAT, new FloatCeilFunction(Collections.singletonList(DataTypes.FLOAT)))
            .put(DataTypes.INTEGER, new FloatCeilFunction(Collections.singletonList(DataTypes.INTEGER)))
            .put(DataTypes.DOUBLE, new DoubleCeilFunction(Collections.singletonList(DataTypes.DOUBLE)))
            .put(DataTypes.LONG, new DoubleCeilFunction(Collections.singletonList(DataTypes.LONG)))
            .put(DataTypes.SHORT, new DoubleCeilFunction(Collections.singletonList(DataTypes.SHORT)))
            .put(DataTypes.BYTE, new DoubleCeilFunction(Collections.singletonList(DataTypes.BYTE)))
            .put(DataTypes.UNDEFINED, new DoubleCeilFunction(Collections.singletonList(DataTypes.UNDEFINED)))
            .build();
        module.register(NAME, new Resolver(NAME, functionMap));
    }

    private static class DoubleCeilFunction extends CeilFunction {

        DoubleCeilFunction(List<DataType> dataTypes) {
            super(generateDoubleFunctionInfo(NAME, dataTypes));
        }

        @Override
        public Long evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return ((Double) Math.ceil(((Number) value).doubleValue())).longValue();
        }

    }

    private static class FloatCeilFunction extends CeilFunction {

        FloatCeilFunction(List<DataType> dataTypes) {
            super(generateFloatFunctionInfo(NAME, dataTypes));
        }

        @Override
        public Integer evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return ((Double) Math.ceil(((Number) value).doubleValue())).intValue();
        }

    }
}
