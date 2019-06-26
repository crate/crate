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

import io.crate.data.Input;

public class LpadRpadFunctionImplHelper {
    private String string1;
    private String string2;
    private int len;
    private int s1len;
    private int s2len;
    private StringBuilder ret;
    private int m;

    public LpadRpadFunctionImplHelper(Input[] args) {

        string1 = (String) args[0].value();
        string2 = " "; // postgres.bki:1050
        len = ((Number) args[1].value()).intValue();
        if (args.length == 3) {
            string2 = (String) args[2].value();

            if (string2 == null) {
                string2 = " "; // default to space if null
            }
        }

        // Negative len is silently taken as zero
        if (len < 0) {
            len = 0;
        }

        s1len = string1.length();

        s2len = string2.length();

        if (s1len > len) {
            s1len = len; /* truncate string1 to len chars */
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
            ret.append(string1.charAt(ptr1));
            ptr1 += 1; // TODO unicode
        }
    }

    public void pad() {
        int ptr2 = 0, ptr2start = 0;
        int ptr2end = ptr2 + s2len;

        while (m-- > 0) {
            ret.append(string2.charAt(ptr2));
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
