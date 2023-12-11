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

package io.crate.expression.scalar.formatting;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.TimeZone;

import org.elasticsearch.common.TriFunction;
import org.jetbrains.annotations.Nullable;
import org.joda.time.Period;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.role.RoleLookup;


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
    }

    private final DataType expressionType;
    private final TriFunction<Object, String, DateTimeFormatter, String> evaluatorFunc;
    @Nullable
    private final DateTimeFormatter formatter;

    public ToCharFunction(Signature signature,
                          BoundSignature boundSignature,
                          TriFunction<Object, String, DateTimeFormatter, String> evaluatorFunc) {
        this(signature, boundSignature, evaluatorFunc, null);
    }

    public ToCharFunction(Signature signature,
                          BoundSignature boundSignature,
                          TriFunction<Object, String, DateTimeFormatter, String> evaluatorFunc,
                          @Nullable DateTimeFormatter formatter) {
        super(signature, boundSignature);

        assert boundSignature.argTypes().size() == 2 : "Number of arguments to to_char must be 2";
        this.expressionType = boundSignature.argTypes().get(0);

        this.evaluatorFunc = evaluatorFunc;
        this.formatter = formatter;
    }

    private static String evaluateTimestamp(Object timestamp, String pattern, @Nullable DateTimeFormatter formatter) {
        if (formatter == null) {
            formatter = new DateTimeFormatter(pattern);
        }
        Long ts = DataTypes.TIMESTAMPZ.sanitizeValue(timestamp);
        LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), TimeZone.getTimeZone("UTC").toZoneId());
        return formatter.format(dateTime);
    }

    private static String evaluateInterval(Object interval, String pattern, @Nullable DateTimeFormatter formatter) {
        if (formatter == null) {
            formatter = new DateTimeFormatter(pattern);
        }
        Period period = DataTypes.INTERVAL.sanitizeValue(interval);
        LocalDateTime dateTime = LocalDateTime.of(0, 1, 1, 0, 0, 0, 0)
            .plusYears(period.getYears())
            .plusMonths(period.getMonths())
            .plusWeeks(period.getWeeks())
            .plusDays(period.getDays())
            .plusHours(period.getHours())
            .plusMinutes(period.getMinutes())
            .plusSeconds(period.getSeconds())
            .plusNanos(period.getMillis() * 1000000);
        return formatter.format(dateTime);
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

        return evaluatorFunc.apply(expression, pattern, formatter);
    }

    @Override
    public Scalar<String, Object> compile(List<Symbol> arguments, String currentUser, RoleLookup userLookup) {
        assert arguments.size() == 2 : "Invalid number of arguments";

        if (!arguments.get(1).symbolType().isValueSymbol()) {
            // arguments are no values, we can't compile
            return this;
        }

        String pattern = (String) ((Input<?>) arguments.get(1)).value();
        DateTimeFormatter formatter = new DateTimeFormatter(pattern);
        return new ToCharFunction(signature, boundSignature, evaluatorFunc, formatter);
    }
}
