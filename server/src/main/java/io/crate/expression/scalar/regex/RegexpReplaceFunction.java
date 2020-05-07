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

package io.crate.expression.scalar.regex;

import io.crate.data.Input;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;

public class RegexpReplaceFunction extends Scalar<String, Object> {

    public static final String NAME = "regexp_replace";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) ->
                new RegexpReplaceFunction(
                    new FunctionInfo(new FunctionIdent(NAME, args), DataTypes.STRING),
                    signature
                )
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature()
            ),
            (signature, args) ->
                new RegexpReplaceFunction(
                    new FunctionInfo(new FunctionIdent(NAME, args), DataTypes.STRING),
                    signature
                )
        );
    }

    private final FunctionInfo info;
    private final Signature signature;
    @Nullable
    private final RegexMatcher regexMatcher;

    private RegexpReplaceFunction(FunctionInfo info, Signature signature) {
        this(info, signature, null);
    }

    private RegexpReplaceFunction(FunctionInfo info, Signature signature, RegexMatcher regexMatcher) {
        this.info = info;
        this.signature = signature;
        this.regexMatcher = regexMatcher;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Nullable
    @Override
    public Signature signature() {
        return signature;
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext txnCtx) {
        List<Symbol> arguments = function.arguments();
        final int size = arguments.size();
        assert size == 3 || size == 4 : "function's number of arguments must be 3 or 4";

        if (anyNonLiterals(arguments)) {
            return function;
        }

        final Input input = (Input) arguments.get(0);
        final Input pattern = (Input) arguments.get(1);
        final Input replacement = (Input) arguments.get(2);
        final String inputValue = (String) input.value();
        final String patternValue = (String) pattern.value();
        final String replacementValue = (String) replacement.value();
        if (inputValue == null || patternValue == null || replacementValue == null) {
            return Literal.NULL;
        }

        String flags = null;
        if (size == 4) {
            flags = (String) ((Input) arguments.get(3)).value();
        }
        return Literal.of(eval(inputValue, patternValue, replacementValue, flags));
    }

    @Override
    public Scalar<String, Object> compile(List<Symbol> arguments) {
        assert arguments.size() >= 3 : "number of arguments muts be > 3";

        Symbol patternSymbol = arguments.get(1);
        if (patternSymbol instanceof Input) {
            String pattern = (String) ((Input) patternSymbol).value();
            if (pattern == null) {
                return this;
            }
            if (arguments.size() == 4) {
                Symbol flagsSymbol = arguments.get(3);
                if (flagsSymbol instanceof Input) {
                    String flags = (String) ((Input) flagsSymbol).value();
                    return new RegexpReplaceFunction(info, signature, new RegexMatcher(pattern, flags));
                }
            }
        }
        return this;
    }

    @Override
    public String evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length == 3 || args.length == 4 : "number of args must be 3 or 4";
        String val = (String) args[0].value();
        String pattern = (String) args[1].value();
        String replacement = (String) args[2].value();
        if (val == null || pattern == null || replacement == null) {
            return null;
        }
        RegexMatcher matcher;
        if (regexMatcher == null) {
            String flags = null;
            if (args.length == 4) {
                flags = (String) args[3].value();
            }
            matcher = new RegexMatcher(pattern, flags);
        } else {
            matcher = regexMatcher;
        }
        return matcher.replace(val, replacement);
    }

    private static String eval(String value, String pattern, String replacement, @Nullable String flags) {
        RegexMatcher regexMatcher = new RegexMatcher(pattern, flags);
        return regexMatcher.replace(value, replacement);
    }
}
