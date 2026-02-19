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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
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


    public enum Token {
        HOUR_OF_DAY("HH"),
        HOUR_OF_DAY_LOWER("hh"),
        HOUR_OF_DAY12("HH12"),
        HOUR_OF_DAY12_LOWER("hh12"),
        HOUR_OF_DAY24("HH24"),
        HOUR_OF_DAY24_LOWER("hh24"),
        MINUTE("MI"),
        MINUTE_LOWER("mi"),
        SECOND("SS"),
        SECOND_LOWER("ss"),
        MILLISECOND("MS"),
        MILLISECOND_LOWER("ms"),
        MICROSECOND("US"),
        MICROSECOND_LOWER("us"),
        TENTH_OF_SECOND("FF1"),
        TENTH_OF_SECOND_LOWER("ff1"),
        HUNDREDTH_OF_SECOND("FF2"),
        HUNDREDTH_OF_SECOND_LOWER("ff2"),
        MILLISECOND_FF("FF3"),
        MILLISECOND_FF_LOWER("ff3"),
        TENTH_OF_MILLISECOND("FF4"),
        TENTH_OF_MILLISECOND_LOWER("ff4"),
        HUNDREDTH_OF_MILLISECOND("FF5"),
        HUNDREDTH_OF_MILLISECOND_LOWER("ff5"),
        MICROSECOND_FF("FF6"),
        MICROSECOND_FF_LOWER("ff6"),
        SECONDS_PAST_MIDNIGHT("SSSS"),
        SECONDS_PAST_MIDNIGHT_LOWER("ssss"),
        SECONDS_PAST_MIDNIGHT_S("SSSSS"),
        SECONDS_PAST_MIDNIGHT_S_LOWER("sssss"),
        AM_UPPER("AM"),
        AM_LOWER("am"),
        PM_UPPER("PM"),
        PM_LOWER("pm"),
        A_M_UPPER("A.M."),
        A_M_LOWER("a.m."),
        P_M_UPPER("P.M."),
        P_M_LOWER("p.m."),
        YEAR_WITH_COMMA("Y,YYY"),
        YEAR_WITH_COMMA_LOWER("y,yyy"),
        YEAR_YYYY("YYYY"),
        YEAR_LOWER_YYYY("yyyy"),
        YEAR_YYY("YYY"),
        YEAR_YYY_LOWER("yyy"),
        YEAR_YY("YY"),
        YEAR_YY_LOWER("yy"),
        YEAR_Y("Y"),
        YEAR_Y_LOWER("y"),
        ISO_YEAR_YYY("IYYY"),
        ISO_YEAR_YYY_LOWER("iyyy"),
        ISO_YEAR_YY("IYY"),
        ISO_YEAR_YY_LOWER("iyy"),
        ISO_YEAR_Y("IY"),
        ISO_YEAR_Y_LOWER("iy"),
        ISO_YEAR("I"),
        ISO_YEAR_LOWER("i"),
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
        MONTH_NUMBER_LOWER("mm"),
        DAY_UPPER("DAY"),
        DAY_CAPITALIZED("Day"),
        DAY_LOWER("day"),
        ABBREVIATED_DAY_UPPER("DY"),
        ABBREVIATED_DAY_CAPITALIZED("Dy"),
        ABBREVIATED_DAY_LOWER("dy"),
        DAY_OF_YEAR("DDD"),
        DAY_OF_YEAR_LOWER("ddd"),
        DAY_OF_ISO_WEEK_NUMBERING_YEAR("IDDD"),
        DAY_OF_ISO_WEEK_NUMBERING_YEAR_LOWER("iddd"),
        DAY_OF_MONTH("DD"),
        DAY_OF_MONTH_LOWER("dd"),
        DAY_OF_WEEK("D"),
        DAY_OF_WEEK_LOWER("d"),
        ISO_DAY_OF_WEEK("ID"),
        ISO_DAY_OF_WEEK_LOWER("id"),
        WEEK_OF_MONTH("W"),
        WEEK_OF_MONTH_LOWER("w"),
        WEEK_NUMBER_OF_YEAR("WW"),
        WEEK_NUMBER_OF_YEAR_LOWER("ww"),
        WEEK_NUMBER_OF_ISO_YEAR("IW"),
        WEEK_NUMBER_OF_ISO_YEAR_LOWER("iw"),
        CENTURY("CC"),
        CENTURY_LOW("cc"),
        JULIAN_DAY("J"),
        JULIAN_DAY_LOWER("j"),
        QUARTER("Q"),
        QUARTER_LOWER("q"),
        ROMAN_MONTH_UPPER("RM"),
        ROMAN_MONTH_LOWER("rm"),
        TIMEZONE_UPPER("TZ"),
        TIMEZONE_LOWER("tz"),
        TIMEZONE_HOURS("TZH"),
        TIMEZONE_HOURS_LOWER("tzh"),
        TIMEZONE_MINUTES("TZM"),
        TIMEZONE_MINUTES_LOWER("tzm"),
        TIMEZONE_OFFSET_FROM_UTC("OF"),
        TIMEZONE_OFFSET_FROM_UTC_LOWER("of");

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
            case HOUR_OF_DAY, HOUR_OF_DAY12, HOUR_OF_DAY_LOWER, HOUR_OF_DAY12_LOWER -> {
                if (datetime.getHour() >= 12) {
                    yield padStart(
                        String.valueOf(datetime.getHour() - 12),
                        2,
                        '0');
                } else {
                    yield padStart(String.valueOf(datetime.getHour()), 2, '0');
                }
            }
            case HOUR_OF_DAY24, HOUR_OF_DAY24_LOWER -> padStart(String.valueOf(datetime.getHour()), 2, '0');
            case MINUTE, MINUTE_LOWER -> padStart(String.valueOf(datetime.getMinute()), 2, '0');
            case SECOND, SECOND_LOWER -> padStart(String.valueOf(datetime.getSecond()), 2, '0');
            case MILLISECOND, MILLISECOND_LOWER -> padStart(String.valueOf(datetime.getNano() / 1000000), 3, '0');
            case MICROSECOND, MICROSECOND_LOWER -> padStart(String.valueOf(datetime.getNano() / 1000), 6, '0');
            case TENTH_OF_SECOND, TENTH_OF_SECOND_LOWER -> datetime.getNano() / 100000000;
            case HUNDREDTH_OF_SECOND, HUNDREDTH_OF_SECOND_LOWER -> datetime.getNano() / 10000000;
            case MILLISECOND_FF, MILLISECOND_FF_LOWER -> datetime.getNano() / 1000000;
            case TENTH_OF_MILLISECOND, TENTH_OF_MILLISECOND_LOWER -> datetime.getNano() / 100000;
            case HUNDREDTH_OF_MILLISECOND, HUNDREDTH_OF_MILLISECOND_LOWER -> datetime.getNano() / 10000;
            case MICROSECOND_FF, MICROSECOND_FF_LOWER -> datetime.getNano() / 1000;
            case SECONDS_PAST_MIDNIGHT, SECONDS_PAST_MIDNIGHT_S, SECONDS_PAST_MIDNIGHT_LOWER, SECONDS_PAST_MIDNIGHT_S_LOWER -> {
                Instant midnight = datetime.toLocalDate().atStartOfDay().toInstant(ZoneOffset.UTC);
                yield String.valueOf(Duration.between(midnight, datetime.toInstant(ZoneOffset.UTC)).getSeconds());
            }
            case AM_UPPER, PM_UPPER -> datetime.getHour() >= 12 ? "PM" : "AM";
            case AM_LOWER, PM_LOWER -> datetime.getHour() >= 12 ? "pm" : "am";
            case A_M_UPPER, P_M_UPPER -> datetime.getHour() >= 12 ? "P.M." : "A.M.";
            case A_M_LOWER, P_M_LOWER -> datetime.getHour() >= 12 ? "p.m." : "a.m.";
            case YEAR_WITH_COMMA, YEAR_WITH_COMMA_LOWER -> {
                String s = String.valueOf(datetime.getYear());
                yield s.substring(0, 1) + "," + s.substring(1);
            }
            case YEAR_YYYY, YEAR_LOWER_YYYY -> padStart(String.valueOf(datetime.getYear()), 4, '0');
            case YEAR_YYY, YEAR_YYY_LOWER -> {
                String s = padStart(String.valueOf(datetime.getYear()), 4, '0');
                yield s.substring(s.length() - 3);
            }
            case YEAR_YY, YEAR_YY_LOWER -> {
                String s = padStart(String.valueOf(datetime.getYear()), 4, '0');
                yield s.substring(s.length() - 2);
            }
            case YEAR_Y, YEAR_Y_LOWER -> {
                String s = padStart(String.valueOf(datetime.getYear()), 4, '0');
                yield s.substring(s.length() - 1);
            }
            case ISO_YEAR_YYY, ISO_YEAR_YYY_LOWER -> String.valueOf(datetime.get(IsoFields.WEEK_BASED_YEAR));
            case ISO_YEAR_YY, ISO_YEAR_YY_LOWER -> {
                String s = String.valueOf(datetime.get(IsoFields.WEEK_BASED_YEAR));
                yield s.substring(s.length() - 3);
            }
            case ISO_YEAR_Y, ISO_YEAR_Y_LOWER -> {
                String s = String.valueOf(datetime.get(IsoFields.WEEK_BASED_YEAR));
                yield s.substring(s.length() - 2);
            }
            case ISO_YEAR, ISO_YEAR_LOWER -> {
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
            case MONTH_NUMBER, MONTH_NUMBER_LOWER -> padStart(
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
            case DAY_OF_YEAR, DAY_OF_YEAR_LOWER -> padStart(
                    String.valueOf(datetime.getDayOfYear()),
                    3,
                    '0'
                );
            case DAY_OF_ISO_WEEK_NUMBERING_YEAR, DAY_OF_ISO_WEEK_NUMBERING_YEAR_LOWER -> padStart(
                    String.valueOf(
                        ((datetime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR) - 1) * 7) + datetime.getDayOfWeek().getValue()),
                    3,
                    '0'
                );
            case DAY_OF_MONTH, DAY_OF_MONTH_LOWER -> padStart(
                    String.valueOf(datetime.getDayOfMonth()),
                    2,
                    '0'
                );
            case DAY_OF_WEEK, DAY_OF_WEEK_LOWER -> (datetime.getDayOfWeek().getValue() % 7) + 1;
            case ISO_DAY_OF_WEEK, ISO_DAY_OF_WEEK_LOWER -> datetime.getDayOfWeek().getValue();
            case WEEK_OF_MONTH, WEEK_OF_MONTH_LOWER -> (datetime.getDayOfMonth() / 7) + 1;
            case WEEK_NUMBER_OF_YEAR, WEEK_NUMBER_OF_YEAR_LOWER -> padStart(
                    String.valueOf(datetime.get(WEEK_OF_YEAR)),
                    2,
                    '0'
                );
            case WEEK_NUMBER_OF_ISO_YEAR, WEEK_NUMBER_OF_ISO_YEAR_LOWER -> padStart(
                    String.valueOf(datetime.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)),
                    2,
                    '0'
                );
            case CENTURY, CENTURY_LOW -> ((datetime.getYear() - 1) / 100) + 1;
            case JULIAN_DAY, JULIAN_DAY_LOWER -> datetime.getLong(JulianFields.JULIAN_DAY);
            case QUARTER, QUARTER_LOWER -> (datetime.getMonthValue() + 2) / 3;
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
            case TIMEZONE_UPPER,
                 TIMEZONE_LOWER,
                 TIMEZONE_HOURS,
                 TIMEZONE_HOURS_LOWER,
                 TIMEZONE_MINUTES,
                 TIMEZONE_MINUTES_LOWER,
                 TIMEZONE_OFFSET_FROM_UTC,
                 TIMEZONE_OFFSET_FROM_UTC_LOWER -> "";
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

    private static class ParsedDateTime {
        Integer year = null;
        Integer month = null;
        Integer day = null;
        Integer hour = null;
        Integer minute = null;
        Integer second = null;
        Integer millisecond = null;
        Integer microsecond = null;
        Boolean isPm = null;
        Integer secondsPastMidnight = null;
        Integer dayOfYear = null;
    }

    private static class ParseResult {
        final int charsConsumed;
        final Object value;

        ParseResult(int charsConsumed, Object value) {
            this.charsConsumed = charsConsumed;
            this.value = value;
        }
    }

    public LocalDateTime parseDateTime(String input) {
        ParsedDateTime parsed = new ParsedDateTime();
        int pos = 0;

        // PostgreSQL behavior: skip leading whitespace
        while (pos < input.length() && Character.isWhitespace(input.charAt(pos))) {
            pos++;
        }

        for (Object tokenObj : this.tokens) {
            if (pos >= input.length()) {
                break;
            }

            if (tokenObj instanceof Token token) {
                ParseResult result = parseToken(token, input, pos);
                pos += result.charsConsumed;
                applyParsedValue(parsed, token, result.value);
            } else {
                // PostgreSQL behavior: separators match flexibly
                // Any non-letter/non-digit in pattern matches any non-letter/non-digit in input
                String literal = String.valueOf(tokenObj);
                pos = skipSeparators(input, pos, literal.length());
            }
        }

        return buildLocalDateTime(parsed);
    }

    private ParseResult parseToken(Token token, String input, int pos) {
        return switch (token) {
            case YEAR_YYYY, YEAR_LOWER_YYYY -> parseDigits(input, pos, 4);
            case YEAR_YYY, YEAR_YYY_LOWER -> parseDigits(input, pos, 3);
            case YEAR_YY, YEAR_YY_LOWER -> parseTwoDigitYear(input, pos);
            case YEAR_Y, YEAR_Y_LOWER -> parseDigits(input, pos, 1);
            case YEAR_WITH_COMMA, YEAR_WITH_COMMA_LOWER -> parseYearWithComma(input, pos);
            case MONTH_NUMBER, MONTH_NUMBER_LOWER -> parseDigits(input, pos, 2);
            case DAY_OF_MONTH, DAY_OF_MONTH_LOWER -> parseDigits(input, pos, 2);
            case HOUR_OF_DAY, HOUR_OF_DAY_LOWER, HOUR_OF_DAY12, HOUR_OF_DAY12_LOWER -> parseDigits(input, pos, 2);
            case HOUR_OF_DAY24, HOUR_OF_DAY24_LOWER -> parseDigits(input, pos, 2);
            case MINUTE, MINUTE_LOWER -> parseDigits(input, pos, 2);
            case SECOND, SECOND_LOWER -> parseDigits(input, pos, 2);
            case MILLISECOND, MILLISECOND_LOWER, MILLISECOND_FF, MILLISECOND_FF_LOWER -> parseDigits(input, pos, 3);
            case MICROSECOND, MICROSECOND_LOWER, MICROSECOND_FF, MICROSECOND_FF_LOWER -> parseMicroseconds(input, pos);
            case TENTH_OF_SECOND, TENTH_OF_SECOND_LOWER -> parseDigits(input, pos, 1);
            case HUNDREDTH_OF_SECOND, HUNDREDTH_OF_SECOND_LOWER -> parseDigits(input, pos, 2);
            case TENTH_OF_MILLISECOND, TENTH_OF_MILLISECOND_LOWER -> parseDigits(input, pos, 4);
            case HUNDREDTH_OF_MILLISECOND, HUNDREDTH_OF_MILLISECOND_LOWER -> parseDigits(input, pos, 5);
            case AM_UPPER, AM_LOWER, PM_UPPER, PM_LOWER -> parseAmPm(input, pos, false);
            case A_M_UPPER, A_M_LOWER, P_M_UPPER, P_M_LOWER -> parseAmPm(input, pos, true);
            case MONTH_UPPER, MONTH_CAPITALIZED, MONTH_LOWER -> parseMonthName(input, pos, TextStyle.FULL);
            case ABBREVIATED_MONTH_UPPER, ABBREVIATED_MONTH_CAPITALIZED, ABBREVIATED_MONTH_LOWER -> parseMonthName(input, pos, TextStyle.SHORT);
            case DAY_UPPER, DAY_CAPITALIZED, DAY_LOWER -> parseDayName(input, pos, TextStyle.FULL);
            case ABBREVIATED_DAY_UPPER, ABBREVIATED_DAY_CAPITALIZED, ABBREVIATED_DAY_LOWER -> parseDayName(input, pos, TextStyle.SHORT);
            case SECONDS_PAST_MIDNIGHT, SECONDS_PAST_MIDNIGHT_LOWER, SECONDS_PAST_MIDNIGHT_S, SECONDS_PAST_MIDNIGHT_S_LOWER -> parseSecondsPastMidnight(input, pos);
            case DAY_OF_YEAR, DAY_OF_YEAR_LOWER -> parseDigits(input, pos, 3);
            case ISO_YEAR_YYY, ISO_YEAR_YYY_LOWER -> parseDigits(input, pos, 4);
            case ISO_YEAR_YY, ISO_YEAR_YY_LOWER -> parseDigits(input, pos, 3);
            case ISO_YEAR_Y, ISO_YEAR_Y_LOWER -> parseDigits(input, pos, 2);
            case ISO_YEAR, ISO_YEAR_LOWER -> parseDigits(input, pos, 1);
            case DAY_OF_WEEK, DAY_OF_WEEK_LOWER, ISO_DAY_OF_WEEK, ISO_DAY_OF_WEEK_LOWER -> parseDigits(input, pos, 1);
            case DAY_OF_ISO_WEEK_NUMBERING_YEAR, DAY_OF_ISO_WEEK_NUMBERING_YEAR_LOWER -> parseDigits(input, pos, 3);
            case WEEK_OF_MONTH, WEEK_OF_MONTH_LOWER -> parseDigits(input, pos, 1);
            case WEEK_NUMBER_OF_YEAR, WEEK_NUMBER_OF_YEAR_LOWER -> parseDigits(input, pos, 2);
            case WEEK_NUMBER_OF_ISO_YEAR, WEEK_NUMBER_OF_ISO_YEAR_LOWER -> parseDigits(input, pos, 2);
            case CENTURY, CENTURY_LOW -> parseDigits(input, pos, 2);
            case JULIAN_DAY, JULIAN_DAY_LOWER -> parseJulianDay(input, pos);
            case QUARTER, QUARTER_LOWER -> parseDigits(input, pos, 1);
            case BC_ERA_UPPER, BC_ERA_LOWER, AD_ERA_UPPER, AD_ERA_LOWER -> parseEra(input, pos, false);
            case B_C_ERA_UPPER, B_C_ERA_LOWER, A_D_ERA_UPPER, A_D_ERA_LOWER -> parseEra(input, pos, true);
            case ROMAN_MONTH_UPPER, ROMAN_MONTH_LOWER -> parseRomanMonth(input, pos);
            case TIMEZONE_UPPER, TIMEZONE_LOWER, TIMEZONE_HOURS, TIMEZONE_HOURS_LOWER,
                 TIMEZONE_MINUTES, TIMEZONE_MINUTES_LOWER, TIMEZONE_OFFSET_FROM_UTC,
                 TIMEZONE_OFFSET_FROM_UTC_LOWER -> new ParseResult(0, null);
        };
    }

    private void applyParsedValue(ParsedDateTime parsed, Token token, Object value) {
        if (value == null) {
            return;
        }
        switch (token) {
            case YEAR_YYYY, YEAR_LOWER_YYYY, YEAR_YYY, YEAR_YYY_LOWER, YEAR_YY, YEAR_YY_LOWER,
                 YEAR_Y, YEAR_Y_LOWER, YEAR_WITH_COMMA, YEAR_WITH_COMMA_LOWER,
                 ISO_YEAR_YYY, ISO_YEAR_YYY_LOWER, ISO_YEAR_YY, ISO_YEAR_YY_LOWER,
                 ISO_YEAR_Y, ISO_YEAR_Y_LOWER, ISO_YEAR, ISO_YEAR_LOWER -> parsed.year = (Integer) value;
            case MONTH_NUMBER, MONTH_NUMBER_LOWER, MONTH_UPPER, MONTH_CAPITALIZED, MONTH_LOWER,
                 ABBREVIATED_MONTH_UPPER, ABBREVIATED_MONTH_CAPITALIZED, ABBREVIATED_MONTH_LOWER,
                 ROMAN_MONTH_UPPER, ROMAN_MONTH_LOWER -> parsed.month = (Integer) value;
            case DAY_OF_MONTH, DAY_OF_MONTH_LOWER -> parsed.day = (Integer) value;
            case HOUR_OF_DAY, HOUR_OF_DAY_LOWER, HOUR_OF_DAY12, HOUR_OF_DAY12_LOWER,
                 HOUR_OF_DAY24, HOUR_OF_DAY24_LOWER -> parsed.hour = (Integer) value;
            case MINUTE, MINUTE_LOWER -> parsed.minute = (Integer) value;
            case SECOND, SECOND_LOWER -> parsed.second = (Integer) value;
            case MILLISECOND, MILLISECOND_LOWER, MILLISECOND_FF, MILLISECOND_FF_LOWER -> parsed.millisecond = (Integer) value;
            case MICROSECOND, MICROSECOND_LOWER, MICROSECOND_FF, MICROSECOND_FF_LOWER -> parsed.microsecond = (Integer) value;
            case TENTH_OF_SECOND, TENTH_OF_SECOND_LOWER -> parsed.millisecond = ((Integer) value) * 100;
            case HUNDREDTH_OF_SECOND, HUNDREDTH_OF_SECOND_LOWER -> parsed.millisecond = ((Integer) value) * 10;
            case TENTH_OF_MILLISECOND, TENTH_OF_MILLISECOND_LOWER -> parsed.microsecond = ((Integer) value) * 100;
            case HUNDREDTH_OF_MILLISECOND, HUNDREDTH_OF_MILLISECOND_LOWER -> parsed.microsecond = ((Integer) value) * 10;
            case AM_UPPER, AM_LOWER, PM_UPPER, PM_LOWER,
                 A_M_UPPER, A_M_LOWER, P_M_UPPER, P_M_LOWER -> parsed.isPm = (Boolean) value;
            case SECONDS_PAST_MIDNIGHT, SECONDS_PAST_MIDNIGHT_LOWER,
                 SECONDS_PAST_MIDNIGHT_S, SECONDS_PAST_MIDNIGHT_S_LOWER -> parsed.secondsPastMidnight = (Integer) value;
            case DAY_OF_YEAR, DAY_OF_YEAR_LOWER -> parsed.dayOfYear = (Integer) value;
            default -> {
            }
        }
    }

    private LocalDateTime buildLocalDateTime(ParsedDateTime parsed) {
        int year = parsed.year != null ? parsed.year : 1;
        int hour = parsed.hour != null ? parsed.hour : 0;
        int minute = parsed.minute != null ? parsed.minute : 0;
        int second = parsed.second != null ? parsed.second : 0;
        int nano = computeNanos(parsed);

        if (parsed.isPm != null && parsed.isPm && hour < 12) {
            hour += 12;
        } else if (parsed.isPm != null && !parsed.isPm && hour == 12) {
            hour = 0;
        }

        if (parsed.secondsPastMidnight != null) {
            hour = parsed.secondsPastMidnight / 3600;
            minute = (parsed.secondsPastMidnight % 3600) / 60;
            second = parsed.secondsPastMidnight % 60;
        }

        if (parsed.dayOfYear != null && parsed.year != null) {
            LocalDate date = LocalDate.ofYearDay(year, parsed.dayOfYear);
            return LocalDateTime.of(date.getYear(), date.getMonth(), date.getDayOfMonth(), hour, minute, second, nano);
        }

        int month = parsed.month != null ? parsed.month : 1;
        int day = parsed.day != null ? parsed.day : 1;
        return LocalDateTime.of(year, month, day, hour, minute, second, nano);
    }

    private int computeNanos(ParsedDateTime parsed) {
        if (parsed.microsecond != null) {
            return parsed.microsecond * 1_000;
        }
        if (parsed.millisecond != null) {
            return parsed.millisecond * 1_000_000;
        }
        return 0;
    }

    private int skipSeparators(String input, int pos, int expectedLength) {
        // PostgreSQL behavior: skip leading whitespace, then match separators flexibly
        // Any sequence of non-alphanumeric chars in input can match any separator in pattern
        int skipped = 0;

        // Skip leading whitespace in input (PostgreSQL skips multiple blanks)
        while (pos + skipped < input.length() && Character.isWhitespace(input.charAt(pos + skipped))) {
            skipped++;
        }

        // If we've consumed whitespace, that may be enough
        if (skipped > 0) {
            return pos + skipped;
        }

        // Otherwise, consume non-alphanumeric characters to match the pattern's separators
        int toConsume = expectedLength;
        while (toConsume > 0 && pos + skipped < input.length()) {
            char c = input.charAt(pos + skipped);
            if (Character.isLetterOrDigit(c)) {
                break;
            }
            skipped++;
            toConsume--;
        }

        return pos + skipped;
    }

    private ParseResult parseDigits(String input, int pos, int maxDigits) {
        int endPos = pos;
        while (endPos < input.length() && endPos < pos + maxDigits && Character.isDigit(input.charAt(endPos))) {
            endPos++;
        }
        if (endPos == pos) {
            return new ParseResult(0, null);
        }
        int value = Integer.parseInt(input.substring(pos, endPos));
        return new ParseResult(endPos - pos, value);
    }

    private ParseResult parseTwoDigitYear(String input, int pos) {
        ParseResult result = parseDigits(input, pos, 2);
        if (result.value == null) {
            return result;
        }
        int twoDigitYear = (Integer) result.value;
        // PostgreSQL uses "nearest to 2020" rule:
        // 00-69 → 2000-2069, 70-99 → 1970-1999
        int fullYear = twoDigitYear < 70 ? 2000 + twoDigitYear : 1900 + twoDigitYear;
        return new ParseResult(result.charsConsumed, fullYear);
    }

    private ParseResult parseYearWithComma(String input, int pos) {
        if (pos >= input.length()) {
            return new ParseResult(0, null);
        }
        int endPos = pos;
        StringBuilder digits = new StringBuilder();
        while (endPos < input.length() && (Character.isDigit(input.charAt(endPos)) || input.charAt(endPos) == ',')) {
            char c = input.charAt(endPos);
            if (c != ',') {
                digits.append(c);
            }
            endPos++;
            if (digits.length() >= 4) {
                break;
            }
        }
        if (digits.length() == 0) {
            return new ParseResult(0, null);
        }
        return new ParseResult(endPos - pos, Integer.parseInt(digits.toString()));
    }

    private ParseResult parseAmPm(String input, int pos, boolean withPeriods) {
        String remaining = input.substring(pos).toUpperCase(Locale.ENGLISH);
        if (withPeriods) {
            if (remaining.startsWith("A.M.")) {
                return new ParseResult(4, false);
            } else if (remaining.startsWith("P.M.")) {
                return new ParseResult(4, true);
            }
        } else {
            if (remaining.startsWith("AM")) {
                return new ParseResult(2, false);
            } else if (remaining.startsWith("PM")) {
                return new ParseResult(2, true);
            }
        }
        return new ParseResult(0, null);
    }

    private ParseResult parseMonthName(String input, int pos, TextStyle style) {
        String remaining = input.substring(pos).toLowerCase(Locale.ENGLISH);
        for (Month m : Month.values()) {
            String name = m.getDisplayName(style, Locale.ENGLISH).toLowerCase(Locale.ENGLISH);
            if (remaining.startsWith(name.trim())) {
                return new ParseResult(name.trim().length(), m.getValue());
            }
        }
        String trimmedRemaining = remaining.trim();
        for (Month m : Month.values()) {
            String name = m.getDisplayName(style, Locale.ENGLISH).toLowerCase(Locale.ENGLISH).trim();
            if (trimmedRemaining.startsWith(name)) {
                int spaceOffset = 0;
                while (pos + spaceOffset < input.length() && input.charAt(pos + spaceOffset) == ' ') {
                    spaceOffset++;
                }
                return new ParseResult(spaceOffset + name.length(), m.getValue());
            }
        }
        return new ParseResult(0, null);
    }

    private ParseResult parseDayName(String input, int pos, TextStyle style) {
        String remaining = input.substring(pos).toLowerCase(Locale.ENGLISH);
        for (java.time.DayOfWeek d : java.time.DayOfWeek.values()) {
            String name = d.getDisplayName(style, Locale.ENGLISH).toLowerCase(Locale.ENGLISH);
            if (remaining.startsWith(name.trim())) {
                return new ParseResult(name.trim().length(), null);
            }
        }
        int spaceOffset = 0;
        while (pos + spaceOffset < input.length() && input.charAt(pos + spaceOffset) == ' ') {
            spaceOffset++;
        }
        String trimmedRemaining = remaining.trim();
        for (java.time.DayOfWeek d : java.time.DayOfWeek.values()) {
            String name = d.getDisplayName(style, Locale.ENGLISH).toLowerCase(Locale.ENGLISH).trim();
            if (trimmedRemaining.startsWith(name)) {
                return new ParseResult(spaceOffset + name.length(), null);
            }
        }
        return new ParseResult(0, null);
    }

    private ParseResult parseSecondsPastMidnight(String input, int pos) {
        int endPos = pos;
        while (endPos < input.length() && Character.isDigit(input.charAt(endPos))) {
            endPos++;
        }
        if (endPos == pos) {
            return new ParseResult(0, null);
        }
        int value = Integer.parseInt(input.substring(pos, endPos));
        return new ParseResult(endPos - pos, value);
    }

    private ParseResult parseJulianDay(String input, int pos) {
        int endPos = pos;
        while (endPos < input.length() && Character.isDigit(input.charAt(endPos))) {
            endPos++;
        }
        if (endPos == pos) {
            return new ParseResult(0, null);
        }
        return new ParseResult(endPos - pos, Integer.parseInt(input.substring(pos, endPos)));
    }

    private ParseResult parseEra(String input, int pos, boolean withPeriods) {
        String remaining = input.substring(pos).toUpperCase(Locale.ENGLISH);
        if (withPeriods) {
            if (remaining.startsWith("A.D") || remaining.startsWith("A.D.")) {
                return new ParseResult(remaining.startsWith("A.D.") ? 4 : 3, true);
            } else if (remaining.startsWith("B.C") || remaining.startsWith("B.C.")) {
                return new ParseResult(remaining.startsWith("B.C.") ? 4 : 3, false);
            }
        } else {
            if (remaining.startsWith("AD")) {
                return new ParseResult(2, true);
            } else if (remaining.startsWith("BC")) {
                return new ParseResult(2, false);
            }
        }
        return new ParseResult(0, null);
    }

    private ParseResult parseRomanMonth(String input, int pos) {
        String remaining = input.substring(pos).toUpperCase(Locale.ENGLISH).trim();
        String[] romans = {"XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I"};
        int[] values = {12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1};
        for (int i = 0; i < romans.length; i++) {
            if (remaining.startsWith(romans[i])) {
                return new ParseResult(romans[i].length(), values[i]);
            }
        }
        return new ParseResult(0, null);
    }

    private ParseResult parseMicroseconds(String input, int pos) {
        int endPos = pos;
        while (endPos < input.length() && endPos < pos + 6 && Character.isDigit(input.charAt(endPos))) {
            endPos++;
        }
        if (endPos == pos) {
            return new ParseResult(0, null);
        }
        StringBuilder digits = new StringBuilder(input.substring(pos, endPos));
        while (digits.length() < 6) {
            digits.append('0');
        }
        int value = Integer.parseInt(digits.toString());
        return new ParseResult(endPos - pos, value);
    }
}
