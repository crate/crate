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
import io.crate.exceptions.InvalidArgumentException;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;


public class ToNumberFunction extends Scalar<Number, String> {

    public static final String NAME = "to_number";

    public static void register(ScalarFunctionModule module) {
        DataTypes.NUMERIC_PRIMITIVE_TYPES.stream()
            .forEach(dataType -> {
                module.register(
                    Signature.scalar(
                        NAME,
                        DataTypes.STRING.getTypeSignature(),
                        DataTypes.STRING.getTypeSignature(),
                        dataType.getTypeSignature()
                    ),
                    ToNumberFunction::new
                );
            });
    }

    private final Signature signature;
    private final Signature boundSignature;

    public ToNumberFunction(Signature signature, Signature boundSignature) {
        this.signature = signature;
        this.boundSignature = boundSignature;
    }

    @Override
    public Number evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<String>... args) {
        String expression = DataTypes.STRING.sanitizeValue(args[0].value());
        String pattern = DataTypes.STRING.sanitizeValue(args[1].value());

        if (expression == null) {
            return null;
        }

        if (pattern == null) {
            return null;
        }

        DecimalFormat formatter = (DecimalFormat) NumberFormat.getNumberInstance(Locale.ENGLISH);
        formatter.applyLocalizedPattern(pattern);

        try {
            return formatter.parse(expression);
        } catch (ParseException e) {
            throw new InvalidArgumentException(e.getMessage());
        }
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
