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

import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.data.Input;
import io.crate.metadata.BaseFunctionResolver;
import io.crate.metadata.functions.params.FuncParams;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionImplementation;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Scalar;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.functions.params.Param;
import io.crate.expression.scalar.ScalarFunctionModule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;

public class MatchesFunction extends Scalar<BytesRef[], Object> {

    public static final String NAME = "regexp_matches";
    private static final DataType ARRAY_STRING_TYPE = new ArrayType(DataTypes.STRING);

    private FunctionInfo info;
    private RegexMatcher regexMatcher;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME,
            new BaseFunctionResolver(
                FuncParams.builder(Param.STRING, Param.STRING)
                    .withVarArgs(Param.STRING).limitVarArgOccurrences(1)
                    .build()) {

                @Override
                public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
                    return new MatchesFunction(
                        new FunctionInfo(new FunctionIdent(NAME, dataTypes), new ArrayType(dataTypes.get(0))));
                }
            });
    }

    private MatchesFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    RegexMatcher regexMatcher() {
        return regexMatcher;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
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
        return Literal.of(evaluate(args), ARRAY_STRING_TYPE);
    }

    @Override
    public Scalar<BytesRef[], Object> compile(List<Symbol> arguments) {
        assert arguments.size() > 1 : "number of arguments must be > 1";
        String pattern = null;
        if (arguments.get(1).symbolType() == SymbolType.LITERAL) {
            Literal literal = (Literal) arguments.get(1);
            Object patternVal = literal.value();
            if (patternVal == null) {
                return this;
            }
            pattern = ((BytesRef) patternVal).utf8ToString();
        }
        BytesRef flags = null;
        if (arguments.size() == 3) {
            assert arguments.get(2).symbolType() == SymbolType.LITERAL :
                "3rd argument must be a " + SymbolType.LITERAL;
            flags = (BytesRef) ((Literal) arguments.get(2)).value();
        }

        if (pattern != null) {
            regexMatcher = new RegexMatcher(pattern, flags);
        } else {
            regexMatcher = null;
        }
        return this;
    }

    @Override
    public BytesRef[] evaluate(Input[] args) {
        assert args.length == 2 || args.length == 3 : "number of args must be 2 or 3";
        Object val = args[0].value();
        final Object patternValue = args[1].value();
        if (val == null || patternValue == null) {
            return null;
        }
        assert patternValue instanceof BytesRef : "patternValue must be BytesRef";
        // value can be a string if e.g. result is retrieved by ESSearchTask
        if (val instanceof String) {
            val = new BytesRef((String) val);
        }

        RegexMatcher matcher;
        if (regexMatcher == null) {
            String pattern = ((BytesRef) patternValue).utf8ToString();
            BytesRef flags = null;
            if (args.length == 3) {
                flags = (BytesRef) args[2].value();
            }
            matcher = new RegexMatcher(pattern, flags);
        } else {
            matcher = regexMatcher;
        }

        if (matcher.match((BytesRef) val)) {
            return matcher.groups();
        }
        return null;
    }
}
