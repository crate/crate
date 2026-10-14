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

import static io.crate.expression.RegexpFlags.isGlobal;
import static io.crate.expression.RegexpFlags.parseFlags;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.Signature.Feature;
import io.crate.role.Roles;
import io.crate.types.DataTypes;

/**
 * PostgreSQL compatible regexp_match scalar function.
 *
 * Returns the substrings from the first match of a POSIX-style regular
 * expression. If the pattern contains capturing groups, the returned array
 * contains those groups; otherwise it contains the complete match.
 */
public final class RegexpMatchFunction extends Scalar<List<String>, Object> {

    public static final String NAME = "regexp_match";

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(DataTypes.STRING.getTypeSignature(), DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpMatchFunction::new
        );
        builder.add(
            Signature.builder(NAME, FunctionType.SCALAR)
                .argumentTypes(
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature(),
                    DataTypes.STRING.getTypeSignature())
                .returnType(DataTypes.STRING_ARRAY.getTypeSignature())
                .features(Feature.DETERMINISTIC, Feature.STRICTNULL)
                .build(),
            RegexpMatchFunction::new
        );
    }

    private RegexpMatchFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    static class CompiledRegexpMatch extends Scalar<List<String>, Object> {

        private final Pattern pattern;

        protected CompiledRegexpMatch(Signature signature, BoundSignature boundSignature, Pattern pattern) {
            super(signature, boundSignature);
            this.pattern = pattern;
        }

        @Override
        public List<String> evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<Object>... args) {
            String value = (String) args[0].value();
            if (value == null) {
                return null;
            }
            return match(value, pattern);
        }
    }

    @Override
    public Scalar<List<String>, Object> compile(List<Symbol> arguments, String currentUser, Roles roles) {
        assert arguments.size() >= 2 && arguments.size() <= 3 : "number of arguments must be 2 or 3";
        Symbol patternSymbol = arguments.get(1);
        if (patternSymbol instanceof Input<?> input) {
            String pattern = (String) input.value();
            if (pattern == null) {
                return this;
            }
            String flags = null;
            if (arguments.size() == 3) {
                Symbol flagsSymbol = arguments.get(2);
                if (!(flagsSymbol instanceof Input<?> flagsInput)) {
                    return this;
                }
                flags = (String) flagsInput.value();
                if (flags == null) {
                    return this;
                }
                if (isGlobal(flags)) {
                    throw new IllegalArgumentException("The regular expression flag is unknown: g");
                }
            }
            return new CompiledRegexpMatch(signature, boundSignature, Pattern.compile(pattern, parseFlags(flags)));
        }
        return this;
    }

    @Override
    public List<String> evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input<Object>[] args) {
        assert args.length == 2 || args.length == 3 : "number of arguments must be 2 or 3";
        String value = (String) args[0].value();
        if (value == null) {
            return null;
        }
        String patternText = (String) args[1].value();
        if (patternText == null) {
            return null;
        }
        String flags = null;
        if (args.length == 3) {
            flags = (String) args[2].value();
            if (flags == null) {
                return null;
            }
            if (isGlobal(flags)) {
                throw new IllegalArgumentException("The regular expression flag is unknown: g");
            }
        }
        Pattern pattern = Pattern.compile(patternText, parseFlags(flags));
        return match(value, pattern);
    }

    private static List<String> match(String value, Pattern pattern) {
        Matcher matcher = pattern.matcher(value);
        if (!matcher.find()) {
            return null;
        }
        int groupCount = matcher.groupCount();
        ArrayList<String> result = new ArrayList<>(Math.max(1, groupCount));
        if (groupCount == 0) {
            result.add(matcher.group());
        } else {
            for (int i = 1; i <= groupCount; i++) {
                result.add(matcher.group(i));
            }
        }
        return result;
    }
}
