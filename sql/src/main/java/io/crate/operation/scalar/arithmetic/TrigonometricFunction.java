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
        SinFunction.registerFunction(module);
        AsinFunction.registerFunction(module);

        CosFunction.registerFunction(module);
        AcosFunction.registerFunction(module);

        TanFunction.registerFunction(module);
        AtanFunction.registerFunction(module);
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

        protected static void registerFunction(ScalarFunctionModule module) {
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

    static class AsinFunction extends TrigonometricFunction {

        public static final String NAME = "asin";

        protected static void registerFunction(ScalarFunctionModule module) {
            // asin(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new AsinFunction(info));
            }
        }

        public AsinFunction(FunctionInfo info) {
            super(info);
        }

        protected Number evaluate(double value) {
            if (value < -1.0 || value > 1.0) {
                throw new IllegalArgumentException("asin(x): is defined only for values in the range of [-1.0, 1.0]");
            } else {
                return Math.asin(value);
            }
        }

    }

    static class CosFunction extends TrigonometricFunction {

        public static final String NAME = "cos";

        protected static void registerFunction(ScalarFunctionModule module) {
            // cos(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new CosFunction(info));
            }
        }

        public CosFunction(FunctionInfo info) {
            super(info);
        }

        protected Number evaluate(double value) {
            return Math.cos(value);
        }

    }

    static class AcosFunction extends TrigonometricFunction {

        public static final String NAME = "acos";

        protected static void registerFunction(ScalarFunctionModule module) {
            // acos(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new AcosFunction(info));
            }
        }

        public AcosFunction(FunctionInfo info) {
            super(info);
        }

        protected Number evaluate(double value) {
            if (value < -1.0 || value > 1.0) {
                throw new IllegalArgumentException("acos(x): is defined only for values in the range of [-1.0, 1.0]");
            } else {
                return Math.acos(value);
            }
        }

    }

    static class TanFunction extends TrigonometricFunction {

        public static final String NAME = "tan";

        protected static void registerFunction(ScalarFunctionModule module) {
            // tan(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new TanFunction(info));
            }
        }

        public TanFunction(FunctionInfo info) {
            super(info);
        }

        protected Number evaluate(double value) {
            return Math.tan(value);
        }

    }

    static class AtanFunction extends TrigonometricFunction {

        public static final String NAME = "atan";

        protected static void registerFunction(ScalarFunctionModule module) {
            // tan(dataType) : double
            for (DataType dt : ALLOWED_TYPES) {
                FunctionInfo info = new FunctionInfo(new FunctionIdent(NAME, Arrays.asList(dt)), DataTypes.DOUBLE);
                module.register(new AtanFunction(info));
            }
        }

        public AtanFunction(FunctionInfo info) {
            super(info);
        }

        protected Number evaluate(double value) {
            if (value < -1.0 || value > 1.0) {
                throw new IllegalArgumentException("atan(x): is defined only for values in the range of [-1.0, 1.0]");
            } else {
                return Math.atan(value);
            }
        }

    }
}
