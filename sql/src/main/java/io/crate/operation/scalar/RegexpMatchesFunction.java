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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.CharsRef;
import org.apache.lucene.util.UnicodeUtil;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexpMatchesFunction extends Scalar<String[], Object> implements DynamicFunctionResolver {

    public static final String NAME = "regexp_matches";

    private static final DataType arrayStringType = new ArrayType(DataTypes.STRING);

    private static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataTypes.STRING);
    }
    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new RegexpMatchesFunction());
    }

    private FunctionInfo info;
    private RegexMatcher regexMatcher;

    private RegexpMatchesFunction() {
    }

    public RegexpMatchesFunction(FunctionInfo info) {
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
    public Scalar<String[], Object> compile(List<Symbol> arguments) {
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
        int flags = 0;
        if (arguments.size() == 3) {
            assert arguments.get(2).symbolType() == SymbolType.LITERAL;
            Object flagsVal = ((Literal) arguments.get(2)).value();
            if (flagsVal != null) {
                flags = parseFlags((BytesRef) flagsVal);
            }
        }

        if (pattern != null) {
            regexMatcher = new RegexMatcher(pattern, flags);
        } else {
            regexMatcher = null;
        }
        return this;
    }

    @Override
    public String[] evaluate(Input[] args) {
        assert (args.length > 1 && args.length < 4);
        Object val = args[0].value();
        final Object patternValue = args[1].value();
        assert patternValue instanceof BytesRef;
        if (val == null || patternValue == null) {
            return null;
        }
        // value can be a string if e.g. result is retrieved by ESSearchTask
        if (val instanceof String) {
            val = new BytesRef((String)val);
        }

        RegexMatcher matcher;
        if (regexMatcher == null) {
            String pattern = ((BytesRef) patternValue).utf8ToString();
            int flags = 0;
            if (args.length == 3) {
                Object flagsVal = args[2].value();
                if (flagsVal != null) {
                    flags = parseFlags((BytesRef) flagsVal);
                }
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

    private int parseFlags(BytesRef flagsString) {
        int flags = 0;
        for (char flag : flagsString.utf8ToString().toCharArray()) {
            switch (flag) {
                case 'i':
                    flags = flags | Pattern.CASE_INSENSITIVE;
                    break;
                case 'u':
                    flags = flags | Pattern.UNICODE_CASE;
                    break;
                case 'U':
                    flags = flags | Pattern.UNICODE_CHARACTER_CLASS;
                    break;
                case 's':
                    flags = flags | Pattern.DOTALL;
                    break;
                case 'm':
                    flags = flags | Pattern.MULTILINE;
                    break;
                case 'x':
                    flags = flags | Pattern.COMMENTS;
                    break;
                case 'd':
                    flags = flags | Pattern.UNIX_LINES;
                    break;
                default:
                    break;
            }
        }

        return flags;
    }

    private static boolean anyNonLiterals(List<Symbol> arguments) {
        for (Symbol symbol : arguments) {
            if (!symbol.symbolType().isValueSymbol()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() > 1 && dataTypes.size() < 4
                && dataTypes.get(0) == DataTypes.STRING && dataTypes.get(1) == DataTypes.STRING);
        if (dataTypes.size() == 3) {
            Preconditions.checkArgument(dataTypes.get(2) == DataTypes.STRING);
        }
        return new RegexpMatchesFunction(createInfo(dataTypes));
    }

    static class RegexMatcher {

        private final Pattern pattern;
        private final Matcher matcher;
        private final CharsRef utf16 = new CharsRef(10);

        public RegexMatcher(String regex, int flags) {
            this.pattern = Pattern.compile(regex, flags);
            this.matcher = this.pattern.matcher(utf16);
        }

        public boolean match(BytesRef term) {
            UnicodeUtil.UTF8toUTF16(term.bytes, term.offset, term.length, utf16);
            return matcher.reset().matches();
        }

        public String[] groups() {
            try {
                String[] groups = new String[matcher.groupCount() + 1];
                for (int i = 0; i <= matcher.groupCount(); i++) {
                    groups[i] = matcher.group(i);
                }
                return groups;
            } catch (IllegalStateException e) {
                // no match -> no groups
            }
            return null;
        }

    }


}
