/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.operator;

import io.crate.metadata.FunctionInfo;
import io.crate.operator.Input;
import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;

import java.util.regex.Pattern;

public class LikeOperator extends Operator<BytesRef> {

    public static final String NAME = "op_like";

    private FunctionInfo info;

    public static final char DEFAULT_ESCAPE = '\\';

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new LikeOperator(generateInfo(NAME, DataType.STRING)));
    }

    public LikeOperator(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        assert (symbol != null);
        assert (symbol.arguments().size() == 2);

        if (!symbol.arguments().get(0).symbolType().isLiteral()) {
            return symbol;
        }

        StringLiteral expression = (StringLiteral) symbol.arguments().get(0);
        StringLiteral pattern = (StringLiteral) symbol.arguments().get(1);

        if (evaluate(expression, pattern)) {
            return BooleanLiteral.TRUE;
        }
        return BooleanLiteral.FALSE;
    }

    @Override
    public Boolean evaluate(Input<BytesRef>... args) {
        assert (args != null);
        assert (args.length == 2);

        BytesRef expression = args[0].value();
        BytesRef pattern = args[1].value();
        if (expression == null || pattern == null) {
            return null;
        }

        return matches(expression.utf8ToString(), pattern.utf8ToString());
    }

    private boolean matches(String expression, String pattern) {
        return Pattern.matches(
                patternToRegex(pattern, DEFAULT_ESCAPE, true),
                expression
        );
    }

    protected static String patternToRegex(String patternString, char escapeChar, boolean shouldEscape) {
        StringBuilder regex = new StringBuilder(patternString.length() * 2);

        regex.append('^');
        boolean escaped = false;
        for (char currentChar : patternString.toCharArray()) {
            if (shouldEscape && !escaped && currentChar == escapeChar) {
                escaped = true;
            } else {
                switch (currentChar) {
                    case '%':
                        if (escaped) {
                            regex.append("%");
                        } else {
                            regex.append(".*");
                        }
                        escaped = false;
                        break;
                    case '_':
                        if (escaped) {
                            regex.append("_");
                        } else {
                            regex.append('.');
                        }
                        escaped = false;
                        break;
                    default:
                        // escape special regex characters
                        switch (currentChar) {
                            case '\\':
                            case '^':
                            case '$':
                            case '.':
                            case '*':
                            case '[':
                            case ']':
                            case '(':
                            case ')':
                                regex.append('\\');
                        }

                        regex.append(currentChar);
                        escaped = false;
                }
            }
        }
        regex.append('$');
        return regex.toString();
    }

}
