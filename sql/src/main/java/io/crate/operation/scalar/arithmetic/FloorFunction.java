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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.data.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public abstract class FloorFunction extends Scalar<Number, Number> {

    public static final String NAME = "floor";

    public static void register(ScalarFunctionModule module) {
        module.register(new DoubleFloorFunction());
        module.register(new FloatFloorFunction());
        module.register(new NoopFloorFunction(DataTypes.LONG));
        module.register(new NoopFloorFunction(DataTypes.INTEGER));
        module.register(new NoopFloorFunction(DataTypes.SHORT));
        module.register(new NoopFloorFunction(DataTypes.BYTE));
        module.register(new NoopFloorFunction(DataTypes.UNDEFINED));
    }

    private static class DoubleFloorFunction extends FloorFunction {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.of(DataTypes.DOUBLE)), DataTypes.LONG, FunctionInfo.Type.SCALAR,
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT);

        @Override
        public Long evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return ((Double) Math.floor(((Number) value).doubleValue())).longValue();
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class FloatFloorFunction extends FloorFunction {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, ImmutableList.of(DataTypes.FLOAT)), DataTypes.INTEGER, FunctionInfo.Type.SCALAR,
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT);

        @Override
        public Integer evaluate(Input[] args) {
            Object value = args[0].value();
            if (value == null) {
                return null;
            }
            return ((Double) Math.floor(((Number) value).doubleValue())).intValue();
        }

        @Override
        public FunctionInfo info() {
            return INFO;
        }
    }

    private static class NoopFloorFunction extends FloorFunction {

        private final FunctionInfo info;

        NoopFloorFunction(DataType type) {
            info = new FunctionInfo(new FunctionIdent(NAME, ImmutableList.of(type)), type, FunctionInfo.Type.SCALAR,
                FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT);
        }

        @Override
        public Number evaluate(Input<Number>[] args) {
            return args[0].value();
        }

        @Override
        public FunctionInfo info() {
            return info;
        }
    }
}
