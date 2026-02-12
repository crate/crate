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

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

public class ToTimestampFunction extends Scalar<Long, Object> {

    public static final String NAME = "to_timestamp";

    public static void register(Functions.Builder module) {
        module.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(), DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.TIMESTAMPZ.getTypeSignature())
                .features(Feature.DETERMINISTIC)
                .build(),
            ToTimestampFunction::new
        );
    }

    @Nullable
    private final DateTimeFormatter formatter;

    public ToTimestampFunction(Signature signature, BoundSignature boundSignature) {
        this(signature, boundSignature, null);
    }

    public ToTimestampFunction(Signature signature, BoundSignature boundSignature, @Nullable DateTimeFormatter formatter) {
        super(signature, boundSignature);
        this.formatter = formatter;
    }

    @SafeVarargs
    @Override
    public final Long evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>... args) {
        String inputString = DataTypes.STRING.sanitizeValue(args[0].value());
        String pattern = DataTypes.STRING.sanitizeValue(args[1].value());

        if (inputString == null || pattern == null) {
            return null;
        }

        DateTimeFormatter dtFormatter = this.formatter;
        if (dtFormatter == null) {
            dtFormatter = new DateTimeFormatter(pattern);
        }

        LocalDateTime dateTime = dtFormatter.parseDateTime(inputString);
        return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
    }

    @Override
    public Scalar<Long, Object> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        assert arguments.size() == 2 : "Invalid number of arguments";

        if (!arguments.get(1).symbolType().isValueSymbol()) {
            // arguments are no values, we can't compile
            return this;
        }

        String pattern = (String) ((Input<?>) arguments.get(1)).value();
        if (pattern == null) {
            return this;
        }
        DateTimeFormatter compiledFormatter = new DateTimeFormatter(pattern);
        return new ToTimestampFunction(signature, boundSignature, compiledFormatter);
    }
}
