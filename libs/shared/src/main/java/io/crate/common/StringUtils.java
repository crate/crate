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


import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public final class StringUtils {

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

    @Nullable
    public static String nullOrString(@Nullable Object value) {
        if (value == null) {
            return null;
        }
        return value.toString();
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
            StringBuilder sb = new StringBuilder(minimumLength);
            sb.append(s);
            for (int i = s.length(); i < minimumLength; i++) {
                sb.append(c);
            }
            return sb.toString();
        }
    }
}
