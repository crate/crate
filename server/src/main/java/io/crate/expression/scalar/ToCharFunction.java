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

package io.crate.expression.scalar;

import io.crate.data.Input;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormat;
import org.joda.time.format.PeriodFormatter;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.function.BiFunction;

public class ToCharFunction extends Scalar<String, Object> {

    public static final String NAME = "to_char";

    public static void register(ScalarFunctionModule module) {
        List.of(DataTypes.TIMESTAMP, DataTypes.TIMESTAMPZ).stream()
            .forEach(type -> {
                module.register(
                    Signature.scalar(
                        NAME,
                        type.getTypeSignature(),
                        DataTypes.STRING.getTypeSignature(),
                        DataTypes.STRING.getTypeSignature()
                    ),
                    (signature, boundSignature) ->
                        new ToCharFunction(
                            signature,
                            boundSignature,
                            ToCharFunction::evaluateTimestamp
                        )
                );
            });

        module.register(
            Signature.scalar(
                NAME,
                DataTypes.INTERVAL.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new ToCharFunction(
                    signature,
                    boundSignature,
                    ToCharFunction::evaluateInterval
                )
        );

        DataTypes.NUMERIC_PRIMITIVE_TYPES.stream()
            .forEach(type -> {
                module.register(
                    Signature.scalar(
                        NAME,
                        type.getTypeSignature(),
                        DataTypes.STRING.getTypeSignature(),
                        DataTypes.STRING.getTypeSignature()
                    ),
                    (signature, boundSignature) ->
                        new ToCharFunction(
                            signature,
                            boundSignature,
                            ToCharFunction::evaluateNumber
                        )
                );
            });
    }

    private final Signature signature;
    private final Signature boundSignature;
    private final DataType expressionType;
    private final BiFunction<Object, String, String> evaluatorFunc;

    private static final PeriodFormatter PERIOD_FORMATTER = PeriodFormat.getDefault();

    public ToCharFunction(Signature signature, Signature boundSignature, BiFunction<Object, String, String> evaluatorFunc) {
        this.signature = signature;
        this.boundSignature = boundSignature;

        assert boundSignature.getArgumentDataTypes().size() == 2 : "Number of arguments to to_char must be 2";
        this.expressionType = boundSignature.getArgumentDataTypes().get(0);

        this.evaluatorFunc = evaluatorFunc;
    }

    private static String evaluateTimestamp(Object timestamp, String pattern) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH);
        Long ts = DataTypes.TIMESTAMPZ.sanitizeValue(timestamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), TimeZone.getTimeZone("UTC").toZoneId());
        return formatter.format(dateTime);
    }

    private static String evaluateInterval(Object interval, String pattern) {
        return PERIOD_FORMATTER.print((Period) interval);
    }

    private static String evaluateNumber(Object number, String pattern) {
        DecimalFormat formatter = (DecimalFormat) NumberFormat.getNumberInstance(Locale.ENGLISH);
        formatter.applyLocalizedPattern(pattern);
        return formatter.format(number);
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        Object expression = expressionType.sanitizeValue(args[0].value());
        String pattern = DataTypes.STRING.sanitizeValue(args[1].value());

        if (expression == null) {
            return null;
        }

        if (pattern == null) {
            return null;
        }

        return evaluatorFunc.apply(expression, pattern);
    }

    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Signature boundSignature() {
        return boundSignature;
    }
}
