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

import com.google.common.collect.ImmutableList;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimeTZType;
import io.crate.types.TimestampType;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BinaryOperator;

public class ArithmeticFunctions {

    private static final Param ARITHMETIC_TYPE = Param.of(
        DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP, DataTypes.UNDEFINED);

    public static class Names {
        public static final String ADD = "add";
        public static final String SUBTRACT = "subtract";
        public static final String MULTIPLY = "multiply";
        public static final String DIVIDE = "divide";
        public static final String POWER = "power";
        public static final String MODULUS = "modulus";
        public static final String MOD = "mod";
    }

    public static void register(ScalarFunctionModule module) {
        module.register(Names.ADD, new ArithmeticFunctionResolver(
            Names.ADD,
            "+",
            FunctionInfo.DETERMINISTIC_AND_COMPARISON_REPLACEMENT,
            Math::addExact,
            Double::sum,
            Math::addExact,
            Float::sum
        ));
        module.register(Names.SUBTRACT, new ArithmeticFunctionResolver(
            Names.SUBTRACT,
            "-",
            FunctionInfo.DETERMINISTIC_ONLY,
            Math::subtractExact,
            (arg0, arg1) -> arg0 - arg1,
            Math::subtractExact,
            (arg0, arg1) -> arg0 - arg1
        ));
        module.register(Names.MULTIPLY, new ArithmeticFunctionResolver(
            Names.MULTIPLY,
            "*",
            FunctionInfo.DETERMINISTIC_ONLY,
            Math::multiplyExact,
            (arg0, arg1) -> arg0 * arg1,
            Math::multiplyExact,
            (arg0, arg1) -> arg0 * arg1
        ));
        module.register(Names.DIVIDE, new ArithmeticFunctionResolver(
            Names.DIVIDE,
            "/",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 / arg1,
            (arg0, arg1) -> arg0 / arg1,
            (arg0, arg1) -> arg0 / arg1,
            (arg0, arg1) -> arg0 / arg1
        ));

        java.util.function.Function<String, ArithmeticFunctionResolver> modFunctionResolverFactory =
            name -> new ArithmeticFunctionResolver(
                name,
                "%",
                FunctionInfo.DETERMINISTIC_ONLY,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1,
                (arg0, arg1) -> arg0 % arg1
            );

        module.register(Names.MODULUS, modFunctionResolverFactory.apply(Names.MODULUS));
        module.register(Names.MOD, modFunctionResolverFactory.apply(Names.MOD));
        module.register(Names.POWER, new DoubleFunctionResolver(
            Names.POWER,
            Math::pow
        ));
    }

    static final class DoubleFunctionResolver extends BaseFunctionResolver {

        private final String name;
        private final BinaryOperator<Double> doubleFunction;

        DoubleFunctionResolver(String name, BinaryOperator<Double> doubleFunction) {
            super(FuncParams.builder(ARITHMETIC_TYPE, ARITHMETIC_TYPE).build());
            this.name = name;
            this.doubleFunction = doubleFunction;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> args) throws IllegalArgumentException {
            return new BinaryScalar<>(doubleFunction, name, DataTypes.DOUBLE, FunctionInfo.DETERMINISTIC_ONLY);
        }
    }

    static final class ArithmeticFunctionResolver extends BaseFunctionResolver {

        private final String name;
        private final String operator;
        private final Set<FunctionInfo.Feature> features;

        private final BinaryOperator<Double> doubleFunction;
        private final BinaryOperator<Integer> integerFunction;
        private final BinaryOperator<Long> longFunction;
        private final BinaryOperator<Float> floatFunction;

        ArithmeticFunctionResolver(String name,
                                   String operator,
                                   Set<FunctionInfo.Feature> features,
                                   BinaryOperator<Integer> integerFunction,
                                   BinaryOperator<Double> doubleFunction,
                                   BinaryOperator<Long> longFunction,
                                   BinaryOperator<Float> floatFunction) {
            super(FuncParams.builder(ARITHMETIC_TYPE, ARITHMETIC_TYPE).build());
            this.name = name;
            this.operator = operator;
            this.doubleFunction = doubleFunction;
            this.integerFunction = integerFunction;
            this.longFunction = longFunction;
            this.floatFunction = floatFunction;
            this.features = features;
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            assert dataTypes.size() == 2 : "Arithmetic operator must receive two arguments";
            DataType<?> fst = dataTypes.get(0);
            DataType<?> snd = dataTypes.get(1);

            if (fst.equals(snd)) {
                final Scalar<?, ?> scalar;
                switch (fst.id()) {
                    case DoubleType.ID:
                        scalar = new BinaryScalar<>(doubleFunction, name, DataTypes.DOUBLE, features);
                        break;

                    case FloatType.ID:
                        scalar = new BinaryScalar<>(floatFunction, name, DataTypes.FLOAT, features);
                        break;

                    case ByteType.ID:
                    case ShortType.ID:
                    case IntegerType.ID:
                        scalar = new BinaryScalar<>(integerFunction, name, DataTypes.INTEGER, features);
                        break;
                    case LongType.ID:
                    case TimestampType.ID_WITH_TZ:
                    case TimestampType.ID_WITHOUT_TZ:
                        scalar = new BinaryScalar<>(longFunction, name, DataTypes.LONG, features);
                        break;

                    default:
                        throw new UnsupportedOperationException(
                            operator + " is not supported on expressions of type " + fst.getName());
                }
                return scalar;
            }

            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Arithmetic operation are not supported for type %s %s", fst, snd));
        }
    }

    public static Function of(String name, Symbol first, Symbol second, Set<FunctionInfo.Feature> features) {
        List<DataType> dataTypes = Arrays.asList(first.valueType(), second.valueType());
        return new Function(
            new FunctionInfo(
                new FunctionIdent(name, dataTypes),
                dataTypes.get(0),
                FunctionInfo.Type.SCALAR,
                features
            ),
            ImmutableList.of(first, second)
        );
    }
}
