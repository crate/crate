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

package io.crate.expression.scalar;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jetbrains.annotations.NotNull;

import org.jetbrains.annotations.VisibleForTesting;
import io.crate.data.Input;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.BoundSignature;
import io.crate.metadata.functions.Signature;
import io.crate.role.Roles;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public class SubstrFunction extends Scalar<String, Object> {

    public static final String NAME = "substr";
    public static final String ALIAS = "substring";

    public static void register(Functions.Builder builder) {
        TypeSignature stringType = DataTypes.STRING.getTypeSignature();
        TypeSignature intType = DataTypes.INTEGER.getTypeSignature();
        for (var name : List.of(NAME, ALIAS)) {
            builder.add(
                Signature.scalar(name, stringType, intType, stringType).withFeature(Feature.DETERMINISTIC),
                SubstrFunction::new
            );
            builder.add(
                Signature.scalar(name, stringType, intType, intType, stringType).withFeature(Feature.DETERMINISTIC),
                SubstrFunction::new
            );
            builder.add(
                Signature.scalar(name, stringType, stringType, stringType).withFeature(Feature.DETERMINISTIC),
                SubstrExtractFunction::new
            );
        }
    }

    private SubstrFunction(Signature signature, BoundSignature boundSignature) {
        super(signature, boundSignature);
    }

    @Override
    public String evaluate(TransactionContext txnCtx, NodeContext nodeCtx, Input[] args) {
        assert args.length == 2 || args.length == 3 : "number of arguments must be 2 or 3";
        String val = (String) args[0].value();
        if (val == null) {
            return null;
        }
        Number beginIdx = (Number) args[1].value();
        if (beginIdx == null) {
            return null;
        }
        if (args.length == 3) {
            Number len = (Number) args[2].value();
            if (len == null) {
                return null;
            }
            return evaluate(val, (beginIdx).intValue(), len.intValue());

        }
        return evaluate(val, (beginIdx).intValue());
    }

    private static String evaluate(@NotNull String inputStr, int beginIdx) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length() - 1) {
            return "";
        }
        int endPos = inputStr.length();
        return substring(inputStr, startPos, endPos);
    }

    @VisibleForTesting
    static String evaluate(@NotNull String inputStr, int beginIdx, int len) {
        final int startPos = Math.max(0, beginIdx - 1);
        if (startPos > inputStr.length() - 1) {
            return "";
        }
        int endPos = inputStr.length();
        if (startPos + len < endPos) {
            endPos = startPos + len;
        }
        return substring(inputStr, startPos, endPos);
    }

    @VisibleForTesting
    static String substring(String value, int begin, int end) {
        return value.substring(begin, end);
    }

    private static class SubstrExtractFunction extends Scalar<String, String> {

        SubstrExtractFunction(Signature signature, BoundSignature boundSignature) {
            super(signature, boundSignature);
        }

        @Override
        @SafeVarargs
        public final String evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<String>... args) {
            String string = args[0].value();
            if (string == null) {
                return null;
            }
            String pattern = args[1].value();
            if (pattern == null) {
                return null;
            }

            Pattern re = Pattern.compile(pattern);
            Matcher matcher = re.matcher(string);
            int groupCount = matcher.groupCount();
            if (matcher.find()) {
                String group = matcher.group(groupCount > 0 ? 1 : 0);
                return group;
            } else {
                return null;
            }
        }

        @Override
        public Scalar<String, String> compile(List<Symbol> arguments, String currentUser, Roles roles) {
            Symbol patternSymbol = arguments.get(1);
            if (patternSymbol instanceof Input<?> input) {
                String pattern = (String) input.value();
                if (pattern == null) {
                    return this;
                }
                return new CompiledSubstr(signature, boundSignature, pattern);
            }
            return this;
        }
    }

    private static class CompiledSubstr extends Scalar<String, String> {

        private final Matcher matcher;

        public CompiledSubstr(Signature signature, BoundSignature boundSignature, String pattern) {
            super(signature, boundSignature);
            this.matcher = Pattern.compile(pattern).matcher("");
        }


        @Override
        @SafeVarargs
        public final String evaluate(TransactionContext txnCtx, NodeContext nodeContext, Input<String>... args) {
            String string = args[0].value();
            if (string == null) {
                return null;
            }
            matcher.reset(string);
            int groupCount = matcher.groupCount();
            if (matcher.find()) {
                String group = matcher.group(groupCount > 0 ? 1 : 0);
                return group;
            } else {
                return null;
            }
        }
    }
}
