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

package io.crate.common;


import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class StringUtils {

    private StringUtils() {}

    public static final ThreadLocal<long[]> PARSE_LONG_BUFFER = ThreadLocal.withInitial(() -> new long[1]);

    public static List<String> splitToList(char delim, String value) {
        ArrayList<String> result = new ArrayList<>();
        int lastStart = 0;
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == delim) {
                result.add(value.substring(lastStart, i));
                lastStart = i + 1;
            }
        }
        if (lastStart <= value.length()) {
            result.add(value.substring(lastStart));
        }
        return result;
    }

    /**
     * Checks if a string is blank within the specified range.
     * {@code start} index is inclusive, {@code end} is exclusive.
     *
     * @return {@code false} if any character within the range is not
     * an unicode space character, otherwise, {@code false}.
     */
    public static boolean isBlank(String string, int start, int end) {
        for (int i = start; i < end; i++) {
            if (!Character.isSpaceChar(string.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public static String camelToSnakeCase(String camelCase) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < camelCase.length(); i++) {
            char c = camelCase.charAt(i);
            if (Character.isUpperCase(c)) {
                if (i > 0) {
                    sb.append('_');
                }
                sb.append(Character.toLowerCase(c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    public static String capitalize(String string) {
        return string.substring(0,1).toUpperCase(Locale.ENGLISH) + string.substring(1).toLowerCase(Locale.ENGLISH);
    }

    public static String padEnd(String s, int minimumLength, char c) {
        if (s == null) {
            throw new NullPointerException("s");
        }
        if (s.length() >= minimumLength) {
            return s;
        } else {
            return s + String.valueOf(c).repeat(minimumLength - s.length());
        }
    }

    public static String trim(String target, char charToTrim) {
        int start = 0;
        int len = target.length();

        while (start < len && target.charAt(start) == charToTrim) {
            start++;
        }

        while (start < len && target.charAt(len - 1) == charToTrim) {
            len--;
        }

        return target.substring(start, len);
    }

    /**
     * This is a copy of {@link Long#parseLong} but optimized to avoid
     * creating an exception and filling its stacktrace if the input cannot be parsed.
     */
    public static boolean tryParseLong(String s, long[] out) {
        boolean negative = false;
        int radix = 10;
        int i = 0, len = s.length();
        long limit = -Long.MAX_VALUE;

        if (len > 0) {
            char firstChar = s.charAt(0);
            if (firstChar < '0') { // Possible leading "+" or "-"
                if (firstChar == '-') {
                    negative = true;
                    limit = Long.MIN_VALUE;
                } else if (firstChar != '+') {
                    return false;
                }

                if (len == 1) { // Cannot have lone "+" or "-"
                    return false;
                }
                i++;
            }
            long multmin = limit / radix;
            long result = 0;
            while (i < len) {
                // Accumulating negatively avoids surprises near MAX_VALUE
                int digit = Character.digit(s.charAt(i++),radix);
                if (digit < 0 || result < multmin) {
                    return false;
                }
                result *= radix;
                if (result < limit + digit) {
                    return false;
                }
                result -= digit;
            }
            out[0] = negative ? result : -result;
            return true;
        } else {
            return false;
        }
    }
}
