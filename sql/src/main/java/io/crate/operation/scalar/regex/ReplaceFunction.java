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

package io.crate.operation.scalar.regex;

import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.Input;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.FunctionResolver;
import io.crate.metadata.Scalar;
import io.crate.metadata.Signature;
import io.crate.metadata.TransactionContext;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.List;

public class ReplaceFunction extends Scalar<BytesRef, Object> implements FunctionResolver {

    public static final String NAME = "regexp_replace";

    private static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.STRING);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new ReplaceFunction());
    }

    private FunctionInfo info;
    private RegexMatcher regexMatcher;

    private ReplaceFunction() {
    }

    private ReplaceFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function, TransactionContext transactionContext) {
        List<Symbol> arguments = function.arguments();
        final int size = arguments.size();
        assert size == 3 || size == 4 : "function's number of arguments must be 3 or 4";

        if (anyNonLiterals(arguments)) {
            return function;
        }

        final Input input = (Input) arguments.get(0);
        final Input pattern = (Input) arguments.get(1);
        final Input replacement = (Input) arguments.get(2);
        final Object inputValue = input.value();
        final Object patternValue = pattern.value();
        final String replacementValue = BytesRefs.toString(replacement.value());
        if (inputValue == null || patternValue == null || replacementValue == null) {
            return Literal.NULL;
        }

        String flags = null;
        if (size == 4) {
            flags = BytesRefs.toString(((Input) arguments.get(3)).value());
        }
        return Literal.of(
            eval(BytesRefs.toBytesRef(inputValue), BytesRefs.toString(patternValue), replacementValue, flags));
    }

    @Override
    public Scalar<BytesRef, Object> compile(List<Symbol> arguments) {
        assert arguments.size() >= 3 : "number of arguments muts be > 3";

        Symbol patternSymbol = arguments.get(1);
        if (patternSymbol instanceof Input) {
            String pattern = BytesRefs.toString(((Input) patternSymbol).value());
            if (pattern == null) {
                return this;
            }
            if (arguments.size() == 4) {
                Symbol flagsSymbol = arguments.get(3);
                if (flagsSymbol instanceof Input) {
                    String flags = BytesRefs.toString(((Input) flagsSymbol).value());
                    regexMatcher = new RegexMatcher(pattern, flags);
                }
            }
        }
        return this;
    }

    @Override
    public BytesRef evaluate(Input[] args) {
        assert args.length == 3 || args.length == 4 : "number of args must be 3 or 4";
        BytesRef val = BytesRefs.toBytesRef(args[0].value());
        String pattern = BytesRefs.toString(args[1].value());
        String replacement = BytesRefs.toString(args[2].value());
        if (val == null || pattern == null || replacement == null) {
            return null;
        }
        RegexMatcher matcher;
        if (regexMatcher == null) {
            String flags = null;
            if (args.length == 4) {
                flags = BytesRefs.toString(args[3].value());
            }
            matcher = new RegexMatcher(pattern, flags);
        } else {
            matcher = regexMatcher;
        }
        return matcher.replace(val, replacement);
    }

    @Override
    public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        return new ReplaceFunction(createInfo(dataTypes));
    }

    @Nullable
    @Override
    public List<DataType> getSignature(List<DataType> dataTypes) {
        if (dataTypes.size() < 3 || dataTypes.size() > 4) {
            return null;
        }
        return Signature.SIGNATURES_ALL_OF_SAME.apply(dataTypes);
    }

    private static BytesRef eval(BytesRef value, String pattern, String replacement, @Nullable String flags) {
        RegexMatcher regexMatcher = new RegexMatcher(pattern, flags);
        return regexMatcher.replace(value, replacement);
    }
}
