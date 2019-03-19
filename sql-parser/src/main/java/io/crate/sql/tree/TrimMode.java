/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.tree;


import java.util.HashSet;
import java.util.Locale;

public enum TrimMode {
    LEADING {
        @Override
        public int getTrimmedLength(String target, HashSet<Character> charsToTrim) {
            return target.length();
        }
    },

    TRAILING {
        @Override
        public int getStartIdx(String target, HashSet<Character> charsToTrim) {
            return 0;
        }
    },
    BOTH;

    public static TrimMode of(String value) {
        return TrimMode.valueOf(value.toUpperCase(Locale.ENGLISH));
    }

    public String value() {
        return name();
    }

    public int getStartIdx(String target, HashSet<Character> charsToTrim) {
        int start = 0;
        int end = target.length();
        while (start < end && charsToTrim.contains(target.charAt(start))) {
            start++;
        }
        return start;
    }

    public int getTrimmedLength(String target, HashSet<Character> charsToTrim) {
        int end = target.length();
        while (0 < end && charsToTrim.contains(target.charAt(end - 1))) {
            end--;
        }
        return end;
    }
}
