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
import io.crate.types.DataTypes;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.Locale;

import static java.time.ZoneOffset.UTC;

public class ToDateFunction extends Scalar<Long, String> {

    public static final String NAME = "to_date";

    public static void register(ScalarFunctionModule module) {
        module.register(
                Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.TIMESTAMPZ.getTypeSignature()
            ),
                ToDateFunction::new
        );
    }

    private final Signature signature;
    private final Signature boundSignature;

    public ToDateFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
        String expression = DataTypes.STRING.sanitizeValue(args[0].value());
        String pattern = DataTypes.STRING.sanitizeValue(args[1].value());

        if (expression == null) {
            return null;
        }

        if (pattern == null) {
            return null;
        }

        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH);
        TemporalAccessor dt = formatter.parse(expression, LocalDate::from);
        LocalDate localDate = LocalDate.from(dt);
        return localDate.atStartOfDay(UTC).toInstant().toEpochMilli();
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
