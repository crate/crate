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

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolType;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class MatchesFunction extends Scalar<BytesRef[], Object> implements DynamicFunctionResolver {

    public static final String NAME = "regexp_matches";

    private static final DataType arrayStringType = new ArrayType(DataTypes.STRING);

    private static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), new ArrayType(types.get(0)));
    }
    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new MatchesFunction());
    }

    private FunctionInfo info;
    private RegexMatcher regexMatcher;

    private MatchesFunction() {
    }

    public MatchesFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    public RegexMatcher regexMatcher() {
        return regexMatcher;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        final int size = symbol.arguments().size();
        assert (size >= 2 && size <= 3);

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
            args[2] = (Input)symbol.arguments().get(2);
        }
        return Literal.newLiteral(evaluate(args), arrayStringType);
    }

    @Override
    public Scalar<BytesRef[], Object> compile(List<Symbol> arguments) {
        assert arguments.size() > 1;
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
            assert arguments.get(2).symbolType() == SymbolType.LITERAL;
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
        assert (args.length > 1 && args.length < 4);
        Object val = args[0].value();
        final Object patternValue = args[1].value();
        if (val == null || patternValue == null) {
            return null;
        }
        assert patternValue instanceof BytesRef;
        // value can be a string if e.g. result is retrieved by ESSearchTask
        if (val instanceof String) {
            val = new BytesRef((String)val);
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

        if (matcher.match((BytesRef)val)) {
            return matcher.groups();
        }
        return null;
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() > 1 && dataTypes.size() < 4
                && dataTypes.get(0) == DataTypes.STRING && dataTypes.get(1) == DataTypes.STRING,
                String.format(Locale.ENGLISH,
                        "[%s] Function implementation not found for argument types %s",
                        NAME, Arrays.toString(dataTypes.toArray())));
        if (dataTypes.size() == 3) {
            Preconditions.checkArgument(dataTypes.get(2) == DataTypes.STRING, "flags must be of type string");
        }
        return new MatchesFunction(createInfo(dataTypes));
    }


}
