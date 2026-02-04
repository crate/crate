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

package io.crate.expression.scalar.regex;

import static io.crate.expression.RegexpFlags.parseFlags;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jspecify.annotations.Nullable;

import io.crate.data.Input;
import io.crate.expression.symbol.Function;
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
import io.crate.types.TypeSignature;

public final class RegexpCountFunction extends Scalar<Integer, Object> {

    public static final String NAME = "regexp_count";

    public static void register(Functions.Builder builder) {
        TypeSignature stringType = DataTypes.STRING.getTypeSignature();
        TypeSignature intType = DataTypes.INTEGER.getTypeSignature();
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(stringType, stringType)
                .returnType(intType)
                .features(Feature.DETERMINISTIC)
                .build(),
            RegexpCountFunction::new
        );
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(stringType, stringType, intType)
                .returnType(intType)
                .features(Feature.DETERMINISTIC)
                .build(),
            RegexpCountFunction::new
        );
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(stringType, stringType, intType, stringType)
                .returnType(intType)
                .features(Feature.DETERMINISTIC)
                .build(),
            RegexpCountFunction::new
        );
    }

    @Nullable
    private final Pattern pattern;

    private RegexpCountFunction(Signature signature, BoundSignature boundSignature) {
        this(signature, boundSignature, null);
    }

    private RegexpCountFunction(Signature signature, BoundSignature boundSignature, @Nullable Pattern pattern) {
        super(signature, boundSignature);
        this.pattern = pattern;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx, NodeContext nodeCtx) {
        return evaluateIfLiterals(this, txnCtx, nodeCtx, symbol);
    }

    @Override
    public Scalar<Integer, Object> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        assert arguments.size() >= 2 : "number of arguments must be >= 2";
        Symbol patternSymbol = arguments.get(1);
        if (patternSymbol instanceof Input<?> input) {
            String pattern = (String) input.value();
            if (pattern == null) {
                return this;
            }
            if (arguments.size() == 4) {
                Symbol flagsSymbol = arguments.get(3);
                if (flagsSymbol instanceof Input<?> flagsInput) {
                    String flags = (String) flagsInput.value();
                    return new RegexpCountFunction(signature, boundSignature, Pattern.compile(pattern, parseFlags(flags)));
                }
                return this;
            }
            return new RegexpCountFunction(signature, boundSignature, Pattern.compile(pattern));
        }
        return this;
    }

    @Override
    public Integer evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length >= 2 && args.length <= 4 : "number of args must be 2 to 4";
        String value = (String) args[0].value();
        String pattern = (String) args[1].value();
        if (value == null || pattern == null) {
            return null;
        }

        int startIndex = 0;
        if (args.length >= 3) {
            Number start = (Number) args[2].value();
            if (start == null) {
                return null;
            }
            startIndex = Math.max(0, start.intValue() - 1);
        }
        if (startIndex > value.length()) {
            return 0;
        }

        String flags = null;
        if (args.length == 4) {
            flags = (String) args[3].value();
        }

        Pattern countPattern;
        if (this.pattern == null) {
            countPattern = Pattern.compile(pattern, parseFlags(flags));
        } else {
            countPattern = this.pattern;
        }

        Matcher matcher = countPattern.matcher(value);
        int count = 0;
        boolean found = matcher.find(startIndex);
        while (found) {
            count++;
            found = matcher.find();
        }
        return count;
    }
}
