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
import io.crate.expression.symbol.SymbolType;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.List;

public class MatchesFunction extends Scalar<List<String>, Object> {

    public static final String NAME = "regexp_matches";

    public static void register(ScalarFunctionModule module) {
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            (signature, args) ->
                new MatchesFunction(
                    new FunctionInfo(new FunctionIdent(NAME, args), DataTypes.STRING_ARRAY),
                    signature
                )
        );
        module.register(
            Signature.scalar(
                NAME,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature()
            ),
            (signature, args) ->
                new MatchesFunction(
                    new FunctionInfo(new FunctionIdent(NAME, args), DataTypes.STRING_ARRAY),
                    signature
                )
        );
    }

    private final FunctionInfo info;
    private final Signature signature;
    @Nullable
    private final RegexMatcher regexMatcher;

    private MatchesFunction(FunctionInfo info,
                            Signature signature) {
        this(info, signature, null);
    }

    private MatchesFunction(FunctionInfo info,
                            Signature signature,
                            @Nullable RegexMatcher regexMatcher) {
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

    RegexMatcher regexMatcher() {
        return regexMatcher;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext txnCtx) {
        final int size = symbol.arguments().size();
        assert size == 2 || size == 3 : "function's number of arguments must be 2 or 3";

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }

        final Symbol input = symbol.arguments().get(0);
        final Symbol pattern = symbol.arguments().get(1);
        final Object inputValue = ((Input) input).value();
        final Object patternValue = ((Input) pattern).value();
        if (inputValue == null || patternValue == null) {
            return Literal.NULL;
        }

        Input[] args = new Input[size];
        args[0] = (Input) input;
        args[1] = (Input) pattern;

        if (size == 3) {
            args[2] = (Input) symbol.arguments().get(2);
        }
        return Literal.of(evaluate(txnCtx, args), DataTypes.STRING_ARRAY);
    }

    @Override
    public Scalar<List<String>, Object> compile(List<Symbol> arguments) {
        assert arguments.size() > 1 : "number of arguments must be > 1";
        String pattern = null;
        if (arguments.get(1).symbolType() == SymbolType.LITERAL) {
            Literal literal = (Literal) arguments.get(1);
            String patternVal = (String) literal.value();
            if (patternVal == null) {
                return this;
            }
            pattern = patternVal;
        }
        String flags = null;
        if (arguments.size() == 3) {
            assert arguments.get(2).symbolType() == SymbolType.LITERAL :
                "3rd argument must be a " + SymbolType.LITERAL;
            flags = (String) ((Literal) arguments.get(2)).value();
        }

        if (pattern != null) {
            return new MatchesFunction(info, signature, new RegexMatcher(pattern, flags));
        }
        return this;
    }

    @Override
    public List<String> evaluate(TransactionContext txnCtx, Input[] args) {
        assert args.length == 2 || args.length == 3 : "number of args must be 2 or 3";
        String val = (String) args[0].value();
        String pattern = (String) args[1].value();
        if (val == null || pattern == null) {
            return null;
        }
        RegexMatcher matcher;
        if (regexMatcher == null) {
            String flags = null;
            if (args.length == 3) {
                flags = (String) args[2].value();
            }
            matcher = new RegexMatcher(pattern, flags);
        } else {
            matcher = regexMatcher;
        }

        if (matcher.match(val)) {
            return matcher.groups();
        }
        return null;
    }
}
