/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.engine.window;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import org.joda.time.DateTime;

import io.crate.expression.scalar.arithmetic.ArithmeticFunctions;
import io.crate.expression.scalar.arithmetic.IntervalTimestampArithmeticScalar;
import io.crate.metadata.functions.BoundSignature;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DateType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.IntervalType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.TimestampType;

// Clamps integral overflow instead of wrapping around.
class WindowFrameBoundaryArithmetic {

    private static final BinaryOperator<Double> ADD_DOUBLE_FUNCTION = Double::sum;
    private static final BinaryOperator<Float> ADD_FLOAT_FUNCTION = Float::sum;
    private static final BinaryOperator<Byte> ADD_BYTE_FUNCTION = (Byte x, Byte y) -> {
        int result = x.intValue() + y.intValue();
        return (byte) Math.clamp(result, Byte.MIN_VALUE, Byte.MAX_VALUE);
    };
    private static final BinaryOperator<Short> ADD_SHORT_FUNCTION = (Short x, Short y) -> {
        int result = x.intValue() + y.intValue();
        return (short) Math.clamp(result, Short.MIN_VALUE, Short.MAX_VALUE);
    };
    private static final BinaryOperator<Integer> ADD_INTEGER_FUNCTION = (Integer x, Integer y) ->
        Math.clamp(x.longValue() + y.longValue(), Integer.MIN_VALUE, Integer.MAX_VALUE);
    private static final BinaryOperator<Long> ADD_LONG_FUNCTION = (Long x, Long y) -> {
        try {
            return Math.addExact(x, y);
        } catch (ArithmeticException e) {
            return y > 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
    };

    private static final BinaryOperator<Double> SUB_DOUBLE_FUNCTION = (arg0, arg1) -> arg0 - arg1;
    private static final BinaryOperator<Float> SUB_FLOAT_FUNCTION = (arg0, arg1) -> arg0 - arg1;
    private static final BinaryOperator<Byte> SUB_BYTE_FUNCTION = (Byte x, Byte y) -> {
        int result = x.intValue() - y.intValue();
        return (byte) Math.clamp(result, Byte.MIN_VALUE, Byte.MAX_VALUE);
    };
    private static final BinaryOperator<Short> SUB_SHORT_FUNCTION = (Short x, Short y) -> {
        int result = x.intValue() - y.intValue();
        return (short) Math.clamp(result, Short.MIN_VALUE, Short.MAX_VALUE);
    };
    private static final BinaryOperator<Integer> SUB_INTEGER_FUNCTION = (Integer x, Integer y) ->
        Math.clamp(x.longValue() - y.longValue(), Integer.MIN_VALUE, Integer.MAX_VALUE);
    private static final BinaryOperator<Long> SUB_LONG_FUNCTION = (Long x, Long y) -> {
        try {
            return Math.subtractExact(x, y);
        } catch (ArithmeticException e) {
            return y < 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
    };

    static BiFunction getAddFunction(DataType<?> fstArgDataType, DataType<?> sndArgDataType) {
        switch (fstArgDataType.id()) {
            case LongType.ID:
            case DateType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                if (IntervalType.ID == sndArgDataType.id()) {
                    var signature = IntervalTimestampArithmeticScalar.signatureFor(
                        fstArgDataType,
                        ArithmeticFunctions.Names.ADD
                    );
                    return new IntervalTimestampArithmeticScalar(
                        DateTime::plus,
                        signature,
                        BoundSignature.sameAsUnbound(signature)
                    );
                }
                return ADD_LONG_FUNCTION;
            case DoubleType.ID:
                return ADD_DOUBLE_FUNCTION;
            case FloatType.ID:
                return ADD_FLOAT_FUNCTION;
            case ByteType.ID:
                return ADD_BYTE_FUNCTION;
            case ShortType.ID:
                return ADD_SHORT_FUNCTION;
            case IntegerType.ID:
                return ADD_INTEGER_FUNCTION;
            default:
                throw new UnsupportedOperationException(
                    "Cannot create add function for data type " + fstArgDataType.getName());
        }
    }

    static BiFunction getSubtractFunction(DataType<?> fstArgDataType, DataType<?> sndArgDataType) {
        switch (fstArgDataType.id()) {
            case LongType.ID:
            case DateType.ID:
            case TimestampType.ID_WITH_TZ:
            case TimestampType.ID_WITHOUT_TZ:
                if (IntervalType.ID == sndArgDataType.id()) {
                    var signature = IntervalTimestampArithmeticScalar.signatureFor(
                        fstArgDataType,
                        ArithmeticFunctions.Names.SUBTRACT
                    );
                    return new IntervalTimestampArithmeticScalar(
                        DateTime::minus,
                        signature,
                        BoundSignature.sameAsUnbound(signature)
                    );
                }
                return SUB_LONG_FUNCTION;
            case DoubleType.ID:
                return SUB_DOUBLE_FUNCTION;
            case FloatType.ID:
                return SUB_FLOAT_FUNCTION;
            case ByteType.ID:
                return SUB_BYTE_FUNCTION;
            case ShortType.ID:
                return SUB_SHORT_FUNCTION;
            case IntegerType.ID:
                return SUB_INTEGER_FUNCTION;
            default:
                throw new UnsupportedOperationException(
                    "Cannot create subtract function for data type " + fstArgDataType.getName());
        }
    }
}
