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

import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.operator.any.AnyLikeOperator;
import io.crate.expression.operator.any.AnyNotLikeOperator;
import io.crate.expression.operator.any.AnyOperator;
import io.crate.lucene.match.CrateRegexQuery;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataTypes;

public class LikeOperators {
    public static final String OP_LIKE = "op_like";
    public static final String OP_ILIKE = "op_ilike";

    public static final String ANY_LIKE = AnyOperator.OPERATOR_PREFIX + "like";
    public static final String ANY_ILIKE = AnyOperator.OPERATOR_PREFIX + "ilike";
    public static final String ANY_NOT_LIKE = AnyOperator.OPERATOR_PREFIX + "not_like";
    public static final String ANY_NOT_ILIKE = AnyOperator.OPERATOR_PREFIX + "not_ilike";

    public static final Character DEFAULT_ESCAPE = '\\';
    /**
     *  To avoid creating a String out of {@link #DEFAULT_ESCAPE} every time
     *  inside the {@link AnyOperator#validateRightArg(Object)}
     */
    public static final String DEFAULT_ESCAPE_STR = "\\";

    public static String likeOperatorName(boolean ignoreCase) {
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
            public Query likeQuery(String fqColumn, String pattern, Character escapeChar, boolean isIndexed) {
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
            public Query likeQuery(String fqColumn, String pattern, Character escapeChar, boolean isIndexed) {
                if (isIndexed) {
                    String regex = patternToRegex(pattern, escapeChar);
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
        public abstract Query likeQuery(String fqColumn, String pattern, Character escapeChar, boolean isIndexed);

        public abstract int patternFlags();
    }

    public static void register(Functions.Builder builder) {
        builder.add(
            Signature.scalar(
                OP_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CaseSensitivity.SENSITIVE)
        );
        builder.add(
            Signature.scalar(
                OP_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CaseSensitivity.INSENSITIVE)
        );

        builder.add(
            Signature.scalar(
                OP_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CaseSensitivity.SENSITIVE)
        );
        builder.add(
            Signature.scalar(
                OP_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ),
            (signature, boundSignature) ->
                new LikeOperator(signature, boundSignature, LikeOperators::matches, CaseSensitivity.INSENSITIVE)
        );
        builder.add(
            Signature.scalar(
                ANY_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.SENSITIVE
                )
        );
        builder.add(
            Signature.scalar(
                ANY_NOT_LIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new AnyNotLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.SENSITIVE
                )
        );
        builder.add(
            Signature.scalar(
                ANY_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new AnyLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.INSENSITIVE
                )
        );
        builder.add(
            Signature.scalar(
                ANY_NOT_ILIKE,
                DataTypes.STRING.getTypeSignature(),
                DataTypes.STRING_ARRAY.getTypeSignature(),
                Operator.RETURN_TYPE.getTypeSignature()
            ).withFeature(Scalar.Feature.NULLABLE),
            (signature, boundSignature) ->
                new AnyNotLikeOperator(
                    signature,
                    boundSignature,
                    CaseSensitivity.INSENSITIVE
                )
        );
    }

    static final Pattern makePattern(String pattern, CaseSensitivity caseSensitivity, Character escape) {
        return Pattern.compile(patternToRegex(pattern, escape), caseSensitivity.patternFlags());
    }

    public static boolean matches(String expression, String pattern, Character escape, CaseSensitivity caseSensitivity) {
        return makePattern(pattern, caseSensitivity, escape).matcher(expression).matches();
    }

    public static String patternToRegex(String patternString, @Nullable Character escapeChar) {
        StringBuilder regex = new StringBuilder(patternString.length() * 2);
        regex.append('^');
        boolean escaped = false;
        for (char currentChar : patternString.toCharArray()) {
            if (escapeChar != null && !escaped && currentChar == escapeChar) {
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
        if (escaped) {
            throwErrorForTrailingEscapeChar(patternString, escapeChar);
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
        if (escaped) {
            throwErrorForTrailingEscapeChar(wildcardString, LikeOperators.DEFAULT_ESCAPE);
        }
        return regex.toString();
    }

    public static void throwErrorForTrailingEscapeChar(String pattern, Character escapeChar) {
        throw new IllegalArgumentException(
            String.format(Locale.ENGLISH, "pattern '%s' must not end with escape character '%s'",
                pattern, escapeChar));
    }
}
