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

package io.crate.expression.scalar.string;

public class StringPadding {
    private final String toPad;
    private String fillerChars;
    private int len;
    private int s1len;
    private final int s2len;
    private final StringBuilder ret;
    private int m;

    public StringPadding(String string, Number length, String fill) {

        toPad = string;
        fillerChars = " "; // postgres.bki:1050
        len = length.intValue();
        fillerChars = fill;
        if (fillerChars == null) {
            fillerChars = " "; // default to space if null
        }

        // Negative len is silently taken as zero
        if (len < 0) {
            len = 0;
        }

        s1len = toPad.length();

        s2len = fillerChars.length();

        if (s1len > len) {
            s1len = len; /* truncate toPad to len chars */
        }

        if (s2len <= 0) {
            len = s1len; /* nothing to pad with, so don't pad */
        }

        int bytelen = len; // TODO unicode

        ret = new StringBuilder(bytelen);

        m = len - s1len;
    }

    public void copyString() {
        int ptr1 = 0;

        while (s1len-- > 0) {
            ret.append(toPad.charAt(ptr1));
            ptr1 += 1; // TODO unicode
        }
    }

    public void pad() {
        int ptr2 = 0, ptr2start = 0;
        int ptr2end = ptr2 + s2len;

        while (m-- > 0) {
            ret.append(fillerChars.charAt(ptr2));
            ptr2 += 1; // TODO unicode

            if (ptr2 == ptr2end) {
                ptr2 = ptr2start;
            }
        }
    }

    public StringBuilder getRet() {
        return ret;
    }
}
