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

package io.crate.expression.scalar.formatting;

import static io.crate.common.StringUtils.padEnd;
import static org.elasticsearch.common.Strings.padStart;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.TextStyle;
import java.time.temporal.IsoFields;
import java.time.temporal.JulianFields;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import io.crate.common.StringUtils;
import io.crate.common.collections.Lists;

public class DateTimeFormatter {

    private enum Token {
        HOUR_OF_DAY("HH"),
        HOUR_OF_DAY12("HH12"),
        HOUR_OF_DAY24("HH24"),
        MINUTE("MI"),
        SECOND("SS"),
        MILLISECOND("MS"),
        MICROSECOND("US"),
        TENTH_OF_SECOND("FF1"),
        HUNDREDTH_OF_SECOND("FF2"),
        MILLISECOND_FF("FF3"),
        TENTH_OF_MILLISECOND("FF4"),
        HUNDREDTH_OF_MILLISECOND("FF5"),
        MICROSECOND_FF("FF6"),
        SECONDS_PAST_MIDNIGHT("SSSS"),
        SECONDS_PAST_MIDNIGHT_S("SSSSS"),
        AM_UPPER("AM"),
        AM_LOWER("am"),
        PM_UPPER("PM"),
        PM_LOWER("pm"),
        A_M_UPPER("A.M."),
        A_M_LOWER("a.m."),
        P_M_UPPER("P.M."),
        P_M_LOWER("p.m."),
        YEAR_WITH_COMMA("Y,YYY"),
        YEAR_YYYY("YYYY"),
        YEAR_YYY("YYY"),
        YEAR_YY("YY"),
        YEAR_Y("Y"),
        ISO_YEAR_YYY("IYYY"),
        ISO_YEAR_YY("IYY"),
        ISO_YEAR_Y("IY"),
        ISO_YEAR("I"),
        BC_ERA_UPPER("BC"),
        BC_ERA_LOWER("bc"),
        AD_ERA_UPPER("AD"),
        AD_ERA_LOWER("ad"),
        B_C_ERA_UPPER("B.C"),
        B_C_ERA_LOWER("b.c"),
        A_D_ERA_UPPER("A.D"),
        A_D_ERA_LOWER("a.d"),
        MONTH_UPPER("MONTH"),
        MONTH_CAPITALIZED("Month"),
        MONTH_LOWER("month"),
        ABBREVIATED_MONTH_UPPER("MON"),
        ABBREVIATED_MONTH_CAPITALIZED("Mon"),
        ABBREVIATED_MONTH_LOWER("mon"),
        MONTH_NUMBER("MM"),
        DAY_UPPER("DAY"),
        DAY_CAPITALIZED("Day"),
        DAY_LOWER("day"),
        ABBREVIATED_DAY_UPPER("DY"),
        ABBREVIATED_DAY_CAPITALIZED("Dy"),
        ABBREVIATED_DAY_LOWER("dy"),
        DAY_OF_YEAR("DDD"),
        DAY_OF_ISO_WEEK_NUMBERING_YEAR("IDDD"),
        DAY_OF_MONTH("DD"),
        DAY_OF_WEEK("D"),
        ISO_DAY_OF_WEEK("ID"),
        WEEK_OF_MONTH("W"),
        WEEK_NUMBER_OF_YEAR("WW"),
        WEEK_NUMBER_OF_ISO_YEAR("IW"),
        CENTURY("CC"),
        JULIAN_DAY("J"),
        QUARTER("Q"),
        ROMAN_MONTH_UPPER("RM"),
        ROMAN_MONTH_LOWER("rm"),
        TIMEZONE_UPPER("TZ"),
        TIMEZONE_LOWER("tz"),
        TIMEZONE_HOURS("TZH"),
        TIMEZONE_MINUTES("TZM"),
        TIMEZONE_OFFSET_FROM_UTC("OF"),;

        private final String token;

        Token(final String token) {
            this.token = token;
        }

        @Override
        public String toString() {
            return this.token;
        }

    }

    static class TokenNode {
        private final Map<Character, TokenNode> children = new HashMap<>();
        private Token token;
        public final TokenNode parent;

        public TokenNode() {
            this.token = null;
            this.parent = null;
        }

        private TokenNode(String tokenString, Token token, TokenNode parent) {
            this.parent = parent;
            if (tokenString.length() == 1) {
                // Last character of the token string, so a terminal leaf.
                this.token = token;
            } else {
                this.token = null;
                this.addChild(tokenString.substring(1), token);
            }
        }

        public void addChild(Token token) {
            this.addChild(token.toString(), token);
        }

        private void addChild(String tokenString, Token token) {
            // If a child for the first character of the string exists, add the rest of the string to it as a child.
            TokenNode child = this.children.get(tokenString.charAt(0));
            if (child == null) {
                this.children.put(tokenString.charAt(0), new TokenNode(tokenString, token, this));
            } else {
                // If the length of token string is 1, the child node is upgraded to a terminal token node.
                if (tokenString.length() == 1) {
                    child.token = token;
                } else {
                    child.addChild(tokenString.substring(1), token);
                }
            }
        }

        public boolean isTokenNode() {
            return this.token != null;
        }

        public boolean isRootNode() {
            return this.parent == null;
        }

    }

    private static final TokenNode ROOT_TOKEN_NODE = new TokenNode();

    static {
        for (Token token: Token.values()) {
            ROOT_TOKEN_NODE.addChild(token);
        }
    }

    private final List<Object> tokens;

    private static final TemporalField WEEK_OF_YEAR = WeekFields.of(Locale.ENGLISH).weekOfWeekBasedYear();

    public DateTimeFormatter(String pattern) {
        this.tokens = DateTimeFormatter.parse(pattern);
    }

    private static List<Object> parse(String pattern) {
        ArrayList<Object> tokens = new ArrayList<>();
        String pattern_to_consume = pattern;
        int idx = 0;

        TokenNode currentTokenNode = ROOT_TOKEN_NODE;
        TokenNode nextTokenNode;

        while (idx <= pattern_to_consume.length() && pattern_to_consume.length() > 0) {

            if (idx == pattern_to_consume.length()) {
                nextTokenNode = null;
            } else {
                nextTokenNode = currentTokenNode.children.get(pattern_to_consume.charAt(idx));
            }

            if (nextTokenNode == null && idx > 0) {
                // No next step along the tree, so the parser should terminate and reset.
                if (currentTokenNode.isTokenNode()) {
                    // If the current node is a token, then a valid token is found and added to the list.
                    // The token will then be removed from the rest of the string to parse.
                    tokens.add(currentTokenNode.token);
                } else {
                    // If the current node is not a token, the parser then traverses back up the tree until it either
                    // finds a token node, or the root.
                    int depth = idx;

                    while (depth > 0) {
                        depth -= 1;
                        currentTokenNode = currentTokenNode.parent;

                        if (currentTokenNode.isTokenNode()) {
                            // We have found the valid token, it should be added to the stack and the remaining rest
                            // of the parsed pattern should be added as a string.
                            tokens.add(currentTokenNode.token);
                            break;
                        }
                    }

                    tokens.add(pattern_to_consume.substring(depth, idx));
                }

                pattern_to_consume = pattern_to_consume.substring(idx);
                currentTokenNode = ROOT_TOKEN_NODE;
                idx = 0;
            } else if (nextTokenNode == null) {
                // If there is no path forward and index is 0, then we're at at the beginning, parsing a non-valid
                // character. Add as a string.
                tokens.add(pattern_to_consume.substring(0, idx + 1));
                pattern_to_consume = pattern_to_consume.substring(idx + 1);
            } else {
                idx += 1;
                currentTokenNode = nextTokenNode;
            }
        }

        return tokens;
    }

    public String format(LocalDateTime datetime) {
        return Lists.joinOn("", this.tokens, x -> {
            if (x instanceof Token) {
                return getElement((Token) x, datetime);
            } else {
                return String.valueOf(x);
            }
        });
    }

    private static String getElement(Token token, LocalDateTime datetime) {
        Object element = switch (token) {
            case HOUR_OF_DAY, HOUR_OF_DAY12 -> {
                if (datetime.getHour() >= 12) {
                    yield padStart(
                        String.valueOf(datetime.getHour() - 12),
                        2,
                        '0');
                } else {
                    yield padStart(String.valueOf(datetime.getHour()), 2, '0');
                }
            }
            case HOUR_OF_DAY24 -> padStart(String.valueOf(datetime.getHour()), 2, '0');
            case MINUTE -> padStart(String.valueOf(datetime.getMinute()), 2, '0');
            case SECOND -> padStart(String.valueOf(datetime.getSecond()), 2, '0');
            case MILLISECOND -> padStart(String.valueOf(datetime.getNano() / 1000000), 3, '0');
            case MICROSECOND -> padStart(String.valueOf(datetime.getNano() / 1000), 6, '0');
            case TENTH_OF_SECOND -> datetime.getNano() / 100000000;
            case HUNDREDTH_OF_SECOND -> datetime.getNano() / 10000000;
            case MILLISECOND_FF -> datetime.getNano() / 1000000;
            case TENTH_OF_MILLISECOND -> datetime.getNano() / 100000;
            case HUNDREDTH_OF_MILLISECOND -> datetime.getNano() / 10000;
            case MICROSECOND_FF -> datetime.getNano() / 1000;
            case SECONDS_PAST_MIDNIGHT, SECONDS_PAST_MIDNIGHT_S -> {
                Instant midnight = datetime.toLocalDate().atStartOfDay().toInstant(ZoneOffset.UTC);
                yield String.valueOf(Duration.between(midnight, datetime.toInstant(ZoneOffset.UTC)).getSeconds());
            }
            case AM_UPPER, PM_UPPER -> datetime.getHour() >= 12 ? "PM" : "AM";
            case AM_LOWER, PM_LOWER -> datetime.getHour() >= 12 ? "pm" : "am";
            case A_M_UPPER, P_M_UPPER -> datetime.getHour() >= 12 ? "P.M." : "A.M.";
            case A_M_LOWER, P_M_LOWER -> datetime.getHour() >= 12 ? "p.m." : "a.m.";
            case YEAR_WITH_COMMA -> {
                String s = String.valueOf(datetime.getYear());
                yield s.substring(0, 1) + "," + s.substring(1);
            }
            case YEAR_YYYY -> padStart(String.valueOf(datetime.getYear()), 4, '0');
            case YEAR_YYY -> {
                String s = padStart(String.valueOf(datetime.getYear()), 4, '0');
                yield s.substring(s.length() - 3);
            }
            case YEAR_YY -> {
                String s = padStart(String.valueOf(datetime.getYear()), 4, '0');
                yield s.substring(s.length() - 2);
            }
            case YEAR_Y -> {
                String s = padStart(String.valueOf(datetime.getYear()), 4, '0');
                yield s.substring(s.length() - 1);
            }
            case ISO_YEAR_YYY -> String.valueOf(datetime.get(IsoFields.WEEK_BASED_YEAR));
            case ISO_YEAR_YY -> {
                String s = String.valueOf(datetime.get(IsoFields.WEEK_BASED_YEAR));
                yield s.substring(s.length() - 3);
            }
            case ISO_YEAR_Y -> {
                String s = String.valueOf(datetime.get(IsoFields.WEEK_BASED_YEAR));
                yield s.substring(s.length() - 2);
            }
            case ISO_YEAR -> {
                String s = String.valueOf(datetime.get(IsoFields.WEEK_BASED_YEAR));
                yield s.substring(s.length() - 1);
            }
            case BC_ERA_UPPER, AD_ERA_UPPER -> datetime.getYear() >= 1 ? "AD" : "BC";
            case BC_ERA_LOWER, AD_ERA_LOWER -> datetime.getYear() >= 1 ? "ad" : "bc";
            case B_C_ERA_UPPER, A_D_ERA_UPPER -> datetime.getYear() >= 1 ? "A.D" : "B.C";
            case B_C_ERA_LOWER, A_D_ERA_LOWER -> datetime.getYear() >= 1 ? "a.d" : "b.c";
            case MONTH_UPPER -> padEnd(
                    datetime.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH),
                    7,' '
                ).toUpperCase(Locale.ENGLISH);
            case MONTH_CAPITALIZED -> StringUtils.capitalize(
                    padEnd(
                        datetime.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH),
                        7, ' '
                    )
                );
            case MONTH_LOWER -> padEnd(
                    datetime.getMonth().getDisplayName(TextStyle.FULL, Locale.ENGLISH),
                    7, ' '
                ).toLowerCase(Locale.ENGLISH);
            case ABBREVIATED_MONTH_UPPER -> datetime.getMonth().getDisplayName(TextStyle.SHORT, Locale.ENGLISH).toUpperCase(Locale.ENGLISH);
            case ABBREVIATED_MONTH_CAPITALIZED -> StringUtils.capitalize(
                    datetime.getMonth().getDisplayName(TextStyle.SHORT, Locale.ENGLISH)
                );
            case ABBREVIATED_MONTH_LOWER -> datetime.getMonth().getDisplayName(TextStyle.SHORT, Locale.ENGLISH).toLowerCase(Locale.ENGLISH);
            case MONTH_NUMBER -> padStart(
                    String.valueOf(datetime.getMonth().getValue()),
                    2,
                    '0'
                );
            case DAY_UPPER -> padEnd(
                    datetime.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH),
                    8, ' '
                ).toUpperCase(Locale.ENGLISH);
            case DAY_CAPITALIZED -> padEnd(
                    StringUtils.capitalize(
                        datetime.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH)
                    ), 8, ' ');
            case DAY_LOWER -> padEnd(
                    datetime.getDayOfWeek().getDisplayName(TextStyle.FULL, Locale.ENGLISH),
                    8,
                    ' '
                ).toLowerCase(Locale.ENGLISH);
            case ABBREVIATED_DAY_UPPER -> datetime.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.ENGLISH).toUpperCase(Locale.ENGLISH);
            case ABBREVIATED_DAY_CAPITALIZED -> StringUtils.capitalize(
                    datetime.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.ENGLISH)
                );
            case ABBREVIATED_DAY_LOWER -> datetime.getDayOfWeek().getDisplayName(TextStyle.SHORT, Locale.ENGLISH).toLowerCase(Locale.ENGLISH);
            case DAY_OF_YEAR -> padStart(
                    String.valueOf(datetime.getDayOfYear()),
                    3,
                    '0'
                );
            case DAY_OF_ISO_WEEK_NUMBERING_YEAR -> padStart(
                    String.valueOf(
                        ((datetime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR) - 1) * 7) + datetime.getDayOfWeek().getValue()),
                    3,
                    '0'
                );
            case DAY_OF_MONTH -> padStart(
                    String.valueOf(datetime.getDayOfMonth()),
                    2,
                    '0'
                );
            case DAY_OF_WEEK -> (datetime.getDayOfWeek().getValue() % 7) + 1;
            case ISO_DAY_OF_WEEK -> datetime.getDayOfWeek().getValue();
            case WEEK_OF_MONTH -> (datetime.getDayOfMonth() / 7) + 1;
            case WEEK_NUMBER_OF_YEAR -> padStart(
                    String.valueOf(datetime.get(WEEK_OF_YEAR)),
                    2,
                    '0'
                );
            case WEEK_NUMBER_OF_ISO_YEAR -> padStart(
                    String.valueOf(datetime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)),
                    2,
                    '0'
                );
            case CENTURY -> ((datetime.getYear() - 1) / 100) + 1;
            case JULIAN_DAY -> datetime.getLong(JulianFields.JULIAN_DAY);
            case QUARTER -> (datetime.getMonthValue() + 2) / 3;
            case ROMAN_MONTH_UPPER -> padEnd(
                    toRoman(datetime.getMonthValue()).toUpperCase(Locale.ENGLISH),
                    4,
                    ' '
                );
            case ROMAN_MONTH_LOWER -> padEnd(
                    toRoman(datetime.getMonthValue()).toLowerCase(Locale.ENGLISH),
                    4,
                    ' '
                );
            case TIMEZONE_UPPER, TIMEZONE_LOWER, TIMEZONE_HOURS, TIMEZONE_MINUTES, TIMEZONE_OFFSET_FROM_UTC -> "";
            default -> "";
        };

        return String.valueOf(element);
    }

    private static String toRoman(int number) {
        int[] romanN = {10, 9, 5, 4, 1};
        String[] romanS = {"X", "IX", "V", "IV", "I"};
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < romanN.length; i++) {
            while (number >= romanN[i]) {
                sb.append(romanS[i]);
                number -= romanN[i];
            }
        }
        return sb.toString();
    }
}
