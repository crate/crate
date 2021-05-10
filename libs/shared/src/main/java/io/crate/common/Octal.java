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

import java.util.Arrays;
import java.util.Locale;

/**
 * Encodes and decodes binary strings into the "bytea" escape format.
 * Unprintable bytes, outside of the 32..126 range are represented with an octal number "\nnn", and backslashes
 * are doubled.
 */
public class Octal {

    private Octal() {
    }

    /**
     * Converts an array of bytes into a String where unprintable characters are represented as octal numbers.
     */
    public static String encode(byte[] data) {
        final StringBuilder sb = new StringBuilder(data.length);

        for (byte b: data) {
            if (b == 92) {
                sb.append("\\\\");
            } else if (b < 32 || b > 126) {
                // unprintable character
                sb.append('\\');
                sb.append((b >> 6) & 0x3);
                sb.append((b >> 3) & 0x7);
                sb.append(b & 0x7);
            } else {
                sb.append((char) b);
            }
        }
        return sb.toString();
    }

    public static byte[] decode(String octalData) {
        final byte[] decodedBytes = new byte[octalData.length()];
        final char[] encodedChars = octalData.toCharArray();
        int decIndex = 0;

        for (int encIndex = 0; encIndex < encodedChars.length; encIndex++) {
            final char c = encodedChars[encIndex];
            if (c == '\\') {
                if (encIndex + 1 >= encodedChars.length) {
                    throwOnTruncatedOctalSequence(encIndex);
                }
                if (encodedChars[encIndex + 1] == '\\') {
                    decodedBytes[decIndex++] = '\\';
                    encIndex++;
                    continue;
                }
                if (encIndex + 3 >= encodedChars.length) {
                    throwOnTruncatedOctalSequence(encIndex);
                }
                decodedBytes[decIndex++] = (byte) (64 * ensureOctalCharacter(encodedChars, encIndex + 1) +
                                                   8 * ensureOctalCharacter(encodedChars, encIndex + 2) +
                                                   ensureOctalCharacter(encodedChars, encIndex + 3));
                encIndex += 3;
            } else {
                decodedBytes[decIndex++] = (byte) c;
            }
        }
        if (decIndex == decodedBytes.length) {
            return decodedBytes;
        }
        return Arrays.copyOf(decodedBytes, decIndex);
    }

    /**
     * Checks the given character is a number in base 8.
     */
    private static int ensureOctalCharacter(char[] chars, int index) {
        final int o = Character.digit(chars[index], 8);
        if (o > 7 || o < 0) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Illegal octal character %s at index %d", chars[index], index)
            );
        }
        return o;
    }

    private static void throwOnTruncatedOctalSequence(int index) {
        throw new IllegalArgumentException(
            String.format(Locale.ENGLISH, "Invalid escape sequence at index %d:" +
                                          " expected 1 or 3 more characters but the end of the string got reached", index)
        );
    }

}
