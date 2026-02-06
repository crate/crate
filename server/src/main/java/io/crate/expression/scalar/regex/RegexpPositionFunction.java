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

import io.crate.common.annotations.VisibleForTesting;
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

public final class RegexpPositionFunction extends Scalar<Integer, Object> {

    public static final String NAME = "regexp_instr";
    @Nullable
    private final Pattern pattern;

    public static void register(Functions.Builder builder) {
        builder.add(
            // regexp_instr ( string text, pattern text )
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(), DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpPositionFunction::new
        );
        builder.add(
            // regexp_instr ( string text, pattern text, start integer )
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpPositionFunction::new
        );
        builder.add(
            // regexp_instr ( string text, pattern text, start integer, N integer )
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpPositionFunction::new
        );
        builder.add(
            // regexp_instr ( string text, pattern text, start integer, N integer, endoption integer )
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpPositionFunction::new
        );
        builder.add(
            // regexp_instr ( string text, pattern text, start integer, N integer, endoption integer, flags TEXT )
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpPositionFunction::new
        );
        builder.add(
            // regexp_instr ( string text, pattern text, start integer, N integer, endoption integer, flags TEXT, subexpr integer )
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.INTEGER.getTypeSignature())
                .returnType(DataTypes.INTEGER.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpPositionFunction::new
        );
    }

    private RegexpPositionFunction(Signature signature, BoundSignature boundSignature) {
        this(signature, boundSignature, null);
    }

    private RegexpPositionFunction(Signature signature, BoundSignature boundSignature, @Nullable Pattern pattern) {
        super(signature, boundSignature);

        this.pattern = pattern;
    }

    @Override
    public Scalar<Integer, Object> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        assert arguments.size() >= 2 : "number of arguments must be between 2 and 7";
        String flags = null;
        Symbol patternSymbol = arguments.get(1);
        if (patternSymbol instanceof Input) {
            String pattern = (String) ((Input<?>) patternSymbol).value();
            if (pattern == null) {
                return this;
            }
            if (arguments.size() >= 6) {
                Symbol flagsSymbol = arguments.get(5);
                if (flagsSymbol instanceof Input) {
                    flags = (String) ((Input<?>) flagsSymbol).value();
                }
            }

            return new RegexpPositionFunction(
                signature, boundSignature, Pattern.compile(pattern, parseFlags(flags)));
        }

        return this;
    }

    @Override
    public Integer evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        assert args.length >= 2 && args.length <= 7 : "number of arguments must be between 2 and 7";
        String value = (String) args[0].value();
        String pattern = (String) args[1].value();
        if (value == null || pattern == null) {
            return null;
        }
        Integer startPosition = 0;
        if (args.length >= 3) {
            startPosition = (Integer) args[2].value();
            if (startPosition == null) {
                return null;
            }
            if (startPosition < 1) {
                throw new IllegalArgumentException("start must be >= 1");
            }
            --startPosition; // 1-based
        }
        Integer N = 1;
        if (args.length >= 4) {
            N = (Integer) args[3].value();
            if (N == null) {
                return null;
            }
            if (N < 1) {
                throw new IllegalArgumentException("N must be >= 1");
            }
        }
        Integer endOption = 0;
        if (args.length >= 5) {
            endOption = (Integer) args[4].value();
            if (endOption == null) {
                return null;
            }
            if (endOption != 0 && endOption != 1) {
                throw new IllegalArgumentException("endoption needs to be set to 0 or 1");
            }
        }
        String flags = "";
        if (args.length >= 6) {
            flags = (String) args[5].value();
            if (flags == null) {
                return null;
            }
        }
        Integer subexpr = 0;
        if (args.length == 7) {
            subexpr = (Integer) args[6].value();
            if (subexpr == null) {
                return null;
            }
            if (subexpr < 0) {
                throw new IllegalArgumentException("subexpr must not be negative");
            }
        }
        Pattern matchPattern = this.pattern;
        if (matchPattern == null) {
            matchPattern = Pattern.compile(pattern, parseFlags(flags));
        }
        Matcher m = matchPattern.matcher(value);
        int result = 0;
        if (m.find(startPosition)) {
            do {
                --N;
                if (N == 0) {
                    // found match
                    if (subexpr <= m.groupCount()) {
                        result = 1 + (endOption == 0 ? m.start(subexpr) : m.end(subexpr));
                    }

                    break;
                }
            } while (m.find());
        }

        return result;
    }

    @VisibleForTesting
    Pattern getPattern() {
        return pattern;
    }
}
