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

package io.crate.expression.scalar.arithmetic;

import java.util.List;
import java.util.function.BiFunction;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;

import io.crate.data.Input;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntervalType;

public class IntervalTimestampArithmeticScalar extends Scalar<Long, Object> implements BiFunction<Long, Period, Long> {

    public static void register(Functions.Builder module) {
        for (var timestampType : List.of(DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ)) {
            module.add(
                Signature.scalar(
                        ArithmeticFunctions.Names.ADD,
                        DataTypes.INTERVAL.getTypeSignature(),
                        timestampType.getTypeSignature(),
                        timestampType.getTypeSignature()
                    ).withFeature(Feature.DETERMINISTIC)
                    .withForbiddenCoercion(),
                (signature, boundSignature) ->
                    new IntervalTimestampArithmeticScalar(
                        "+",
                        signature,
                        boundSignature
                    )
            );
            module.add(
                signatureFor(timestampType, ArithmeticFunctions.Names.ADD),
                (signature, boundSignature) ->
                    new IntervalTimestampArithmeticScalar(
                        "+",
                        signature,
                        boundSignature
                    )
            );

            module.add(
                signatureFor(timestampType, ArithmeticFunctions.Names.SUBTRACT),
                (signature, boundSignature) ->
                    new IntervalTimestampArithmeticScalar(
                        "-",
                        signature,
                        boundSignature
                    )
            );
        }
    }

    public static Signature signatureFor(DataType<?> timestampType, String name) {
        return Signature.scalar(
                name,
                timestampType.getTypeSignature(),
                DataTypes.INTERVAL.getTypeSignature(),
                timestampType.getTypeSignature()
            ).withFeature(Feature.DETERMINISTIC)
            .withForbiddenCoercion();
    }

    private final BiFunction<DateTime, Period, DateTime> operation;
    private final int periodIdx;
    private final int timestampIdx;

    public IntervalTimestampArithmeticScalar(String operator,
                                             Signature signature,
                                             BoundSignature boundSignature) {
        super(signature, boundSignature);
        var firstArgType = boundSignature.argTypes().get(0);
        if (firstArgType.id() == IntervalType.ID) {
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
                if (firstArgType.id() == IntervalType.ID) {
                    throw new IllegalArgumentException("Unsupported operator for interval " + operator);
                }
                operation = DateTime::minus;
                break;
            default:
                operation = (a, b) -> {
                    throw new IllegalArgumentException("Unsupported operator for interval " + operator);
                };
        }
    }

    @Override
    public Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        final Long timestamp = (Long) args[timestampIdx].value();
        final Period period = (Period) args[periodIdx].value();
        return apply(timestamp, period);
    }

    @Override
    public Long apply(Long timestamp, Period period) {
        if (period == null || timestamp == null) {
            return null;
        }
        return operation.apply(new DateTime(timestamp, DateTimeZone.UTC), period).toInstant().getMillis();
    }
}
