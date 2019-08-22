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
import io.crate.common.collections.Lists2;
import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.functions.params.Param;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IntervalType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;
import org.elasticsearch.common.util.set.Sets;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public class ArithmeticFunctions {

    private static final Param ARITHMETIC_TYPE = Param.of(
        DataTypes.NUMERIC_PRIMITIVE_TYPES, DataTypes.TIMESTAMPZ, DataTypes.TIMESTAMP, DataTypes.INTERVAL, DataTypes.UNDEFINED);

    public static class Names {
        public static final String ADD = "add";
        public static final String SUBTRACT = "subtract";
        public static final String MULTIPLY = "multiply";
        public static final String DIVIDE = "divide";
        public static final String POWER = "power";
        public static final String MODULUS = "modulus";
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
        module.register(Names.MODULUS, new ArithmeticFunctionResolver(
            Names.MODULUS,
            "%",
            FunctionInfo.DETERMINISTIC_ONLY,
            (arg0, arg1) -> arg0 % arg1,
            (arg0, arg1) -> arg0 % arg1,
            (arg0, arg1) -> arg0 % arg1,
            (arg0, arg1) -> arg0 % arg1
        ));
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

        @Nullable
        @Override
        public List<DataType> getSignature(List<? extends FuncArg> dataTypes) {
            if (dataTypes.size() == 2) {
                DataType fst = dataTypes.get(0).valueType();
                DataType snd = dataTypes.get(1).valueType();

                if ((isInterval(fst) && isTimestamp(snd)) ||
                    (isTimestamp(fst) && isInterval(snd))) {
                    return Lists2.map(dataTypes, FuncArg::valueType);
                }
            }
            return super.getSignature(dataTypes);
        }

        @Override
        public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            assert dataTypes.size() == 2 : "Arithmetic operator must receive two arguments";

            DataType fst = dataTypes.get(0);
            DataType snd = dataTypes.get(1);

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

                    case IntervalType.ID:
                        scalar = new IntervalArithmeticScalar(operator, name);
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
                return Scalar.withOperator(scalar, operator);
            }

            if (isInterval(fst) && isTimestamp(snd)) {
                return new IntervalTimestampScalar(operator, name, fst, snd, snd);
            }
            if (isTimestamp(fst) && isInterval(snd)) {
                return new IntervalTimestampScalar(operator, name, fst, snd, fst);
            }

            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Arithmetic operation are not supported for type %s %s", fst, snd));
        }

        private static boolean isInterval(DataType d) {
            return d.id() == IntervalType.ID;
        }

        private static boolean isTimestamp(DataType d) {
            return TIMESTAMP_IDS.contains(d.id());
        }

        static final Set<Integer> TIMESTAMP_IDS = Sets.newHashSet(DataTypes.TIMESTAMP.id(),
                                                                  DataTypes.TIMESTAMPZ.id());

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

    private static final class IntervalArithmeticScalar extends Scalar<Period, Object> {

        private final FunctionInfo info;
        private final BiFunction<Period, Period, Period> operation;

        IntervalArithmeticScalar(String operator, String name) {
            this.info = new FunctionInfo(
                new FunctionIdent(name, Arrays.asList(DataTypes.INTERVAL, DataTypes.INTERVAL)), DataTypes.INTERVAL);
            switch (operator) {
                case "+":
                    operation = Period::plus;
                    break;
                case "-":
                    operation = Period::minus;
                    break;
                default:
                    operation = (a,b) -> {
                        throw new IllegalArgumentException("Unsupported operator for interval " + operator);
                    };
            }
        }

        @Override
        public Period evaluate(TransactionContext txnCtx, Input<Object>... args) {
            Period fst = (Period) args[0].value();
            Period snd = (Period) args[1].value();

            if (fst == null || snd == null) {
                return null;
            }
            return operation.apply(fst, snd);
        }

        @Override
        public FunctionInfo info() {
            return this.info;
        }
    }

    private static final class IntervalTimestampScalar extends Scalar<Long, Object> {

        private final BiFunction<DateTime, Period, DateTime> operation;
        private final FunctionInfo info;
        private final int periodIdx;
        private final int timestampIdx;

        IntervalTimestampScalar(String operator, String name, DataType firstType, DataType secondType, DataType returnType) {
            this.info = new FunctionInfo(new FunctionIdent(name, Arrays.asList(firstType, secondType)), returnType);
            if (ArithmeticFunctionResolver.isInterval(firstType)) {
                periodIdx = 0;
                timestampIdx = 1;
            } else {
                periodIdx = 1;
                timestampIdx = 0;
            }

            switch (operator) {
                case "+":
                    operation = DateTime::plus;
                    break;
                case "-":
                    if (ArithmeticFunctionResolver.isInterval(firstType)) {
                        throw new IllegalArgumentException("Unsupported operator for interval " + operator);
                    }
                    operation = DateTime::minus;
                    break;
                default:
                    operation = (a,b) -> {
                        throw new IllegalArgumentException("Unsupported operator for interval " + operator);
                    };
            }
        }

        @Override
        public Long evaluate(TransactionContext txnCtx, Input<Object>... args) {
            final Long timestamp = (Long) args[timestampIdx].value();
            final Period period = (Period) args[periodIdx].value();

            if (period == null || timestamp == null) {
                return null;
            }
            return operation.apply(new DateTime(timestamp, DateTimeZone.UTC), period).toInstant().getMillis();
        }

        @Override
        public FunctionInfo info() {
            return this.info;
        }
    }
}
