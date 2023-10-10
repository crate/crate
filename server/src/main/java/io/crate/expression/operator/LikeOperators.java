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

import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.operator.any.AnyLikeOperator;
import io.crate.expression.operator.any.AnyNotLikeOperator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

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

    public enum CaseSensitivity {
        SENSITIVE {

            @Override
            public Query likeQuery(String fqColumn, String pattern, boolean isIndexed) {
                if (isIndexed) {
                    String luceneWildcard = convertSqlLikeToLuceneWildcard(pattern);
                    Term term = new Term(fqColumn, luceneWildcard);
                    return new WildcardQuery(term);
                }
                return null;
            }

            @Override
            public int patternFlags() {
                return Pattern.DOTALL;
            }

        },
        INSENSITIVE {

            @Override
            public Query likeQuery(String fqColumn, String pattern, boolean isIndexed) {
                if (isIndexed) {
                    String regex = patternToRegex(pattern);
                    Term term = new Term(fqColumn, regex);
                    return new CrateRegexQuery(term, Pattern.CASE_INSENSITIVE | Pattern.UNICODE_CASE);
                }
                return null;
            }

            @Override
            public int patternFlags() {
                return Pattern.DOTALL | Pattern.CASE_INSENSITIVE;
            }
        };

        @Nullable
        public abstract Query likeQuery(String fqColumn, String pattern, boolean isIndexed);

        public abstract int patternFlags();
    }

    public static void register(OperatorModule module) {
        module.register(
            Signature.scalar(
                OP_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CaseSensitivity.SENSITIVE)
        );
        module.register(
            Signature.scalar(
                OP_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CaseSensitivity.INSENSITIVE)
        );
        module.register(
            Signature.scalar(
                ANY_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.SENSITIVE
                )
        );
        module.register(
            Signature.scalar(
                ANY_NOT_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new AnyNotLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.SENSITIVE
                )
        );
        module.register(
            Signature.scalar(
                ANY_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.INSENSITIVE
                )
        );
        module.register(
            Signature.scalar(
                ANY_NOT_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new AnyNotLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.INSENSITIVE
                )
        );
    }

    static final Pattern makePattern(String pattern, CaseSensitivity caseSensitivity) {
        return Pattern.compile(patternToRegex(pattern, DEFAULT_ESCAPE, true), caseSensitivity.patternFlags());
    }

    public static boolean matches(String expression, String pattern, CaseSensitivity caseSensitivity) {
        return makePattern(pattern, caseSensitivity).matcher(expression).matches();
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
                            case '{':
                            case '}':
                            case '?':
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

    public static String convertSqlLikeToLuceneWildcard(String wildcardString) {
        // lucene uses * and ? as wildcard characters
        // but via SQL they are used as % and _
        // here they are converted back.
        StringBuilder regex = new StringBuilder();

        boolean escaped = false;
        for (char currentChar : wildcardString.toCharArray()) {
            if (!escaped && currentChar == LikeOperators.DEFAULT_ESCAPE) {
                escaped = true;
            } else {
                switch (currentChar) {
                    case '%':
                        regex.append(escaped ? '%' : '*');
                        escaped = false;
                        break;
                    case '_':
                        regex.append(escaped ? '_' : '?');
                        escaped = false;
                        break;
                    default:
                        switch (currentChar) {
                            case '\\':
                            case '*':
                            case '?':
                                regex.append('\\');
                                break;
                            default:
                        }
                        regex.append(currentChar);
                        escaped = false;
                }
            }
        }
        return regex.toString();
    }
}
