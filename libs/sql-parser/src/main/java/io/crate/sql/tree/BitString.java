/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.sql.tree;

import java.util.BitSet;

import io.crate.sql.ExpressionFormatter;

public record BitString(BitSet bitSet, int length) implements Literal, Comparable<BitString> {

    public static BitString ofBitString(String bitString) {
        assert bitString.startsWith("B'") : "Bitstring must start with B'";
        assert bitString.endsWith("'") : "Bitstrign must end with '";
        return ofRawBits(bitString.substring(2, bitString.length() - 1));
    }

    public static BitString ofRawBits(String bits) {
        return ofRawBits(bits, bits.length());
    }

    public static BitString ofRawBits(String bits, int length) {
        BitSet bitSet = toBitSet(bits, length);
        return new BitString(bitSet, length);
    }

    private static BitSet toBitSet(String text, int length) {
        BitSet bitSet = new BitSet(length);
        for (int i = 0; i < length; i++) {
            char c = text.charAt(i);
            boolean value = switch (c) {
                case '0' -> false;
                case '1' -> true;
                default -> throw new IllegalArgumentException(
                    "Bit string must only contain `0` or `1` values. Encountered: " + c);
            };
            bitSet.set(i, value);
        }
        return bitSet;
    }

    /**
     * Return bits as string with B' prefix. Example:
     *
     * <pre>
     *  B'1101'
     * </pre>
     */
    public String asPrefixedBitString() {
        var sb = new StringBuilder("B'");
        for (int i = 0; i < length; i++) {
            sb.append(bitSet.get(i) ? '1' : '0');
        }
        sb.append("'");
        return sb.toString();
    }

    /**
     * Return bits as string without B' prefix. Example:
     *
     * <pre>
     *  1101
     * </pre>
     */
    public String asRawBitString() {
        var sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(bitSet.get(i) ? '1' : '0');
        }
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBitString(this, context);
    }

    @Override
    public final String toString() {
        return ExpressionFormatter.formatStandaloneExpression(this);
    }

    @Override
    public int compareTo(BitString o) {
        // This is basically a lexicographically comparison on the bit string
        // This matches the PostgreSQL behavior (See `bit_cmp` implementation in PostgreSQL)

        int smallerLength = Math.min(length, o.length);
        for (int i = 0; i < smallerLength; i++) {
            boolean thisSet = bitSet.get(i);
            boolean otherSet = o.bitSet.get(i);
            int compare = Boolean.compare(thisSet, otherSet);
            if (compare == 0) {
                continue;
            }
            return compare;
        }
        return Integer.compare(length, o.length);
    }
}
