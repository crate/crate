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

package io.crate.expression.scalar.arithmetic;

import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.Param;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.UndefinedType;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public abstract class NegateFunction<TOut, TIn> extends Scalar<TOut, TIn> {

    private final FunctionInfo info;
    private static final String NAME = "_negate";

    private NegateFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    private static final FunctionResolver RESOLVER = new FunctionResolver() {

        private final FuncParams funcParams = FuncParams.builder(Param.NUMERIC).build();

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            DataType dataType = dataTypes.get(0);
            switch (dataType.id()) {
                case DoubleType.ID:
                    return new NegateDouble();
                case FloatType.ID:
                    return new NegateFloat();
                case ShortType.ID:
                    return new NegateShort();
                case IntegerType.ID:
                    return new NegateInteger();
                case LongType.ID:
                    return new NegateLong();
                default:
                    throw new IllegalArgumentException("Cannot negate values of type " + dataType.getName());
            }
        }

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> dataTypes) {
            return funcParams.match(dataTypes);
        }
    };

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, RESOLVER);
    }

    public static Function createFunction(Symbol argument) {
        FunctionInfo info;
        switch (argument.valueType().id()) {
            case DoubleType.ID:
                info = NegateDouble.INFO;
                break;
            case FloatType.ID:
                info = NegateFloat.INFO;
                break;
            case ShortType.ID:
                info = NegateShort.INFO;
                break;
            case IntegerType.ID:
                info = NegateInteger.INFO;
                break;
            case LongType.ID:
                info = NegateLong.INFO;
                break;
            case UndefinedType.ID:
                info = NegateLong.INFO;
                break;
            default:
                throw new IllegalArgumentException("Cannot negate values of type " + argument.valueType().getName());
        }
        //noinspection ArraysAsListWithZeroOrOneArgument # must use mutable list for arguments
        return new Function(info, Arrays.asList(argument));
    }

    private static class NegateLong extends NegateFunction<Long, Long> {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, Collections.<DataType>singletonList(DataTypes.LONG)), DataTypes.LONG);

        NegateLong() {
            super(INFO);
        }

        @SafeVarargs
        @Override
        public final Long evaluate(Input<Long>... args) {
            Long value = args[0].value();
            if (value == null) {
                return null;
            }
            return value * -1;
        }
    }

    private static class NegateDouble extends NegateFunction<Double, Double> {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, Collections.<DataType>singletonList(DataTypes.DOUBLE)), DataTypes.DOUBLE);

        NegateDouble() {
            super(INFO);
        }

        @SafeVarargs
        @Override
        public final Double evaluate(Input<Double>... args) {
            Double value = args[0].value();
            if (value == null) {
                return null;
            }
            return value * -1;
        }
    }

    private static class NegateFloat extends NegateFunction<Float, Float> {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, Collections.<DataType>singletonList(DataTypes.FLOAT)), DataTypes.FLOAT);

        NegateFloat() {
            super(INFO);
        }

        @SafeVarargs
        @Override
        public final Float evaluate(Input<Float>... args) {
            Float value = args[0].value();
            if (value == null) {
                return null;
            }
            return value * -1;
        }
    }

    private static class NegateShort extends NegateFunction<Short, Short> {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, Collections.<DataType>singletonList(DataTypes.SHORT)), DataTypes.SHORT);

        NegateShort() {
            super(INFO);
        }

        @SafeVarargs
        @Override
        public final Short evaluate(Input<Short>... args) {
            Short value = args[0].value();
            if (value == null) {
                return null;
            }
            return (short)(value * -1);
        }
    }

    private static class NegateInteger extends NegateFunction<Integer, Integer> {

        private static final FunctionInfo INFO = new FunctionInfo(
            new FunctionIdent(NAME, Collections.<DataType>singletonList(DataTypes.INTEGER)), DataTypes.INTEGER);

        NegateInteger() {
            super(INFO);
        }

        @SafeVarargs
        @Override
        public final Integer evaluate(Input<Integer>... args) {
            Integer value = args[0].value();
            if (value == null) {
                return null;
            }
            return value * -1;
        }
    }
}
