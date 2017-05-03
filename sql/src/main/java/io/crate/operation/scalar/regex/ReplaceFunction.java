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
import io.crate.analyze.symbol.SymbolType;
import io.crate.data.Input;
import io.crate.metadata.*;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

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
    public Symbol normalizeSymbol(Function symbol, TransactionContext transactionContext) {
        final int size = symbol.arguments().size();
        assert size == 3 || size == 4 : "function's number of arguments must be 3 or 4";

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }

        final Symbol input = symbol.arguments().get(0);
        final Symbol pattern = symbol.arguments().get(1);
        final Symbol replacement = symbol.arguments().get(2);
        final Object inputValue = ((Input) input).value();
        final Object patternValue = ((Input) pattern).value();
        final Object replacementValue = ((Input) replacement).value();
        if (inputValue == null || patternValue == null || replacementValue == null) {
            return Literal.NULL;
        }

        Input[] args = new Input[size];
        args[0] = (Input) input;
        args[1] = (Input) pattern;
        args[2] = (Input) replacement;

        if (size == 4) {
            args[3] = (Input) symbol.arguments().get(3);
        }
        return Literal.of(evaluate(args));
    }

    @Override
    public Scalar<BytesRef, Object> compile(List<Symbol> arguments) {
        assert arguments.size() >= 3 : "number of arguments muts be > 3";
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
        if (arguments.size() == 4) {
            assert arguments.get(3).symbolType() == SymbolType.LITERAL
                : "4th argument must be of type " + SymbolType.LITERAL;
            flags = (BytesRef) ((Literal) arguments.get(3)).value();
        }

        if (pattern != null) {
            regexMatcher = new RegexMatcher(pattern, flags);
        } else {
            regexMatcher = null;
        }
        return this;
    }

    @Override
    public BytesRef evaluate(Input[] args) {
        assert args.length == 3 || args.length == 4 : "number of args must be 3 or 4";
        Object val = args[0].value();
        Object patternValue = args[1].value();
        Object replacementValue = args[2].value();
        if (val == null || patternValue == null || replacementValue == null) {
            return null;
        }
        // values can be strings if e.g. result is retrieved by ESSearchTask
        if (val instanceof String) {
            val = new BytesRef((String) val);
        }
        if (replacementValue instanceof String) {
            replacementValue = new BytesRef((String) replacementValue);
        }
        if (patternValue instanceof BytesRef) {
            patternValue = ((BytesRef) patternValue).utf8ToString();
        }

        RegexMatcher matcher;
        if (regexMatcher == null) {
            BytesRef flags = null;
            if (args.length == 4) {
                flags = (BytesRef) args[3].value();
            }
            matcher = new RegexMatcher((String) patternValue, flags);
        } else {
            matcher = regexMatcher;
        }

        return matcher.replace((BytesRef) val, (BytesRef) replacementValue);
    }

    @Override
    public FunctionImplementation getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        return new ReplaceFunction(createInfo(dataTypes));
    }

    @Nullable
    @Override
    public List<DataType> getSignature(List<DataType> dataTypes) {
        if (dataTypes.size() < 3 || dataTypes.size() > 4){
            return null;
        }
        return Signature.SIGNATURES_ALL_OF_SAME.apply(dataTypes);
    }
}
