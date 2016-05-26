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

import com.google.common.collect.ImmutableSet;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Arrays;
import java.util.Set;

public abstract class TrigonometricFunction extends Scalar<Number, Number> {

    private static final Set<DataType> ALLOWED_TYPES = ImmutableSet.<DataType>builder()
        .addAll(DataTypes.NUMERIC_PRIMITIVE_TYPES)
        .add(DataTypes.UNDEFINED)
        .build();

    public static void register(ScalarFunctionModule module) {
        SinFunction.registerSinFunction(module);
    }

    private final FunctionInfo info;

    public TrigonometricFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Number evaluate(Input<Number>... args) {
        assert args.length == 1;
        if (args[0].value() == null) {
            return null;
        }
        double value = args[0].value().doubleValue();
        return evaluate(value);
    }

    abstract protected Number evaluate(double value);

    static class SinFunction extends TrigonometricFunction {

        public static final String NAME = "sin";

        protected static void registerSinFunction(ScalarFunctionModule module) {
            // sin(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new SinFunction(info));
            }
        }

        public SinFunction(FunctionInfo info) {
            super(info);
        }

        protected Number evaluate(double value) {
            return Math.sin(value);
        }

    }
}
