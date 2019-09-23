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

package io.crate.expression.operator;

import com.google.common.collect.ImmutableList;
import io.crate.data.Input;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.Scalar;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.regex.Pattern;

public class LikeOperator extends Operator<String> {

    public static final String NAME = "op_like";
    public static final char DEFAULT_ESCAPE = '\\';

    private FunctionInfo info;

    public static void register(OperatorModule module) {
        module.registerOperatorFunction(new LikeOperator(
            new FunctionInfo(new FunctionIdent(NAME,
                                               ImmutableList.of(DataTypes.STRING,
                                                                DataTypes.STRING,
                                                                DataTypes.BOOLEAN)),
                             RETURN_TYPE)));
    }

    public LikeOperator(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Scalar<Boolean, String> compile(List<Symbol> arguments) {
        Symbol pattern = arguments.get(1);
        if (pattern instanceof Input) {
            Object value = ((Input) pattern).value();
            if (value == null) {
                return this;
            }
            boolean ignoreCase = ((Literal<Boolean>) arguments.get(2)).value();
            return new CompiledLike(info, (String) value, ignoreCase);
        }
        return super.compile(arguments);
    }

    @Override
    public Boolean evaluate(TransactionContext txnCtx, Input<String>... args) {
        assert args != null : "args must not be null";
        assert args.length == 3 : "number of args must be 3";

        String expression = args[0].value();
        String pattern = args[1].value();
        if (expression == null || pattern == null) {
            return null;
        }
        boolean ignoreCase = (Boolean) ((Object) args[2].value());
        return matches(expression, pattern, ignoreCase);
    }

    protected static Pattern makePattern(String pattern, boolean ignoreCase) {
        int flags = Pattern.DOTALL;
        if (ignoreCase) {
            flags |= Pattern.CASE_INSENSITIVE;
        }
        return Pattern.compile(patternToRegex(pattern, DEFAULT_ESCAPE, true), flags);
    }

    protected static boolean matches(String expression, Pattern pattern) {
        return pattern.matcher(expression).matches();
    }

    public static boolean matches(String expression, String pattern, boolean ignoreCase) {
        return matches(expression, makePattern(pattern, ignoreCase));
    }

    public static String patternToRegex(String patternString) {
        return patternToRegex(patternString, DEFAULT_ESCAPE, true);
    }

    public static String patternToRegex(String patternString, char escapeChar, boolean shouldEscape) {
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
                            // fall through
                            case '\\':
                            case '^':
                            case '$':
                            case '.':
                            case '*':
                            case '[':
                            case ']':
                            case '(':
                            case ')':
                            case '|':
                            case '+':
                                regex.append('\\');
                                break;
                            default:
                        }

                        regex.append(currentChar);
                        escaped = false;
                }
            }
        }
        regex.append('$');
        return regex.toString();
    }

    private static class CompiledLike extends Scalar<Boolean, String> {
        private final FunctionInfo info;
        private final Pattern pattern;

        CompiledLike(FunctionInfo info, String pattern, boolean ignoreCase) {
            this.info = info;
            this.pattern = makePattern(pattern, ignoreCase);
        }

        @Override
        public FunctionInfo info() {
            return info;
        }

        @SafeVarargs
        @Override
        public final Boolean evaluate(TransactionContext txnCtx, Input<String>... args) {
            String value = args[0].value();
            if (value == null) {
                return null;
            }
            return matches(value, pattern);
        }
    }
}
