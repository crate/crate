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

package io.crate.expression.operator;

import io.crate.expression.operator.any.AnyLikeOperator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

import java.util.regex.Pattern;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class LikeOperators {
    public static final String OP_LIKE = "op_like";
    public static final String OP_ILIKE = "op_ilike";

    public static final String ANY_LIKE = AnyOperator.OPERATOR_PREFIX + "like";
    public static final String ANY_ILIKE = AnyOperator.OPERATOR_PREFIX + "ilike";
    public static final String ANY_NOT_LIKE = AnyOperator.OPERATOR_PREFIX + "not_like";
    public static final String ANY_NOT_ILIKE = AnyOperator.OPERATOR_PREFIX + "not_ilike";

    public static final char DEFAULT_ESCAPE = '\\';

    public static String arrayOperatorName(boolean ignoreCase) {
        return ignoreCase ? OP_ILIKE : OP_LIKE;
    }

    public static String arrayOperatorName(boolean negate, boolean ignoreCase) {
        String name;
        if (negate) {
            name = ignoreCase ? ANY_NOT_ILIKE : ANY_NOT_LIKE;
        } else {
            name = ignoreCase ? ANY_ILIKE : ANY_LIKE;
        }
        return name;
    }

    private static final int CASE_SENSITIVE = Pattern.DOTALL;
    private static final int CASE_INSENSITIVE = Pattern.DOTALL | Pattern.CASE_INSENSITIVE;

    public static void register(OperatorModule module) {
        module.register(
            Signature.scalar(
                OP_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CASE_SENSITIVE)
        );
        module.register(
            Signature.scalar(
                OP_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CASE_INSENSITIVE)
        );
        module.register(
            Signature.scalar(
                ANY_LIKE,
                parseTypeSignature("E"),
                parseTypeSignature("array(E)"),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    LikeOperators::matches,
                    CASE_SENSITIVE
                )
        );
        module.register(
            Signature.scalar(
                ANY_NOT_LIKE,
                parseTypeSignature("E"),
                parseTypeSignature("array(E)"),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    TriPredicate.negate(LikeOperators::matches),
                    CASE_SENSITIVE
                )
        );
        module.register(
            Signature.scalar(
                ANY_ILIKE,
                parseTypeSignature("E"),
                parseTypeSignature("array(E)"),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    LikeOperators::matches,
                    CASE_INSENSITIVE
                )
        );
        module.register(
            Signature.scalar(
                ANY_NOT_ILIKE,
                parseTypeSignature("E"),
                parseTypeSignature("array(E)"),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withTypeVariableConstraints(typeVariable("E")),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    TriPredicate.negate(LikeOperators::matches),
                    CASE_INSENSITIVE
                )
        );
    }

    static final Pattern makePattern(String pattern, int flags) {
        return Pattern.compile(patternToRegex(pattern, DEFAULT_ESCAPE, true), flags);
    }

    static boolean matches(String expression, String pattern, int patternMatchingFlags) {
        return makePattern(pattern, patternMatchingFlags).matcher(expression).matches();
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
}
