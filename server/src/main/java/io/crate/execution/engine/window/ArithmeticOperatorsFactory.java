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

package io.crate.execution.engine.window;

import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.arithmetic.IntervalTimestampArithmeticScalar;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IntervalType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimeZType;
import io.crate.types.TimestampType;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

class ArithmeticOperatorsFactory {

    private static final BinaryOperator<Double> ADD_DOUBLE_FUNCTION = Double::sum;
    private static final BinaryOperator<Integer> ADD_INTEGER_FUNCTION = Integer::sum;
    private static final BinaryOperator<Long> ADD_LONG_FUNCTION = Long::sum;
    private static final BinaryOperator<Float> ADD_FLOAT_FUNCTION = Float::sum;

    private static final BinaryOperator<Double> SUB_DOUBLE_FUNCTION = (arg0, arg1) -> arg0 - arg1;
    private static final BinaryOperator<Integer> SUB_INTEGER_FUNCTION = (arg0, arg1) -> arg0 - arg1;
    private static final BinaryOperator<Long> SUB_LONG_FUNCTION = (arg0, arg1) -> arg0 - arg1;
    private static final BinaryOperator<Float> SUB_FLOAT_FUNCTION = (arg0, arg1) -> arg0 - arg1;

    static BiFunction getAddFunction(DataType fstArgDataType, DataType sndArgDataType) {
        switch (fstArgDataType.id()) {
            case LongType.ID:
            case TimeZType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                if (IntervalType.ID == sndArgDataType.id()) {
                    return new IntervalTimestampArithmeticScalar(
                        "+",
                        ArithmeticFunctions.Names.ADD,
                        List.of(fstArgDataType, sndArgDataType),
                        fstArgDataType,
                        IntervalTimestampArithmeticScalar.signatureFor(fstArgDataType, ArithmeticFunctions.Names.ADD)
                    );
                }
                return ADD_LONG_FUNCTION;
            case DoubleType.ID:
                return ADD_DOUBLE_FUNCTION;
            case FloatType.ID:
                return ADD_FLOAT_FUNCTION;
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
                return ADD_INTEGER_FUNCTION;
            default:
                throw new UnsupportedOperationException(
                    "Cannot create add function for data type " + fstArgDataType.getName());
        }
    }

    static BiFunction getSubtractFunction(DataType fstArgDataType, DataType sndArgDataType) {
        switch (fstArgDataType.id()) {
            case LongType.ID:
            case TimeZType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                if (IntervalType.ID == sndArgDataType.id()) {
                    return new IntervalTimestampArithmeticScalar(
                        "-",
                        ArithmeticFunctions.Names.SUBTRACT,
                        List.of(fstArgDataType, sndArgDataType),
                        fstArgDataType,
                        IntervalTimestampArithmeticScalar.signatureFor(fstArgDataType, ArithmeticFunctions.Names.SUBTRACT)
                    );
                }
                return SUB_LONG_FUNCTION;
            case DoubleType.ID:
                return SUB_DOUBLE_FUNCTION;
            case FloatType.ID:
                return SUB_FLOAT_FUNCTION;
            case ByteType.ID:
            case ShortType.ID:
            case IntegerType.ID:
                return SUB_INTEGER_FUNCTION;
            default:
                throw new UnsupportedOperationException(
                    "Cannot create subtract function for data type " + fstArgDataType.getName());
        }
    }
}
