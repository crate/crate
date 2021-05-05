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

public class BitString extends Literal {

    public static BitString of(String text) {
        int length = text.length();
        BitSet bitSet = new BitSet(length);
        for (int i = 0; i < length; i++) {
            char c = text.charAt(i);
            boolean value = switch (c) {
                case '0' -> false;
                case '1' -> true;
                default -> {
                    throw new IllegalArgumentException("Bit string must only contain `0` or `1` values. Encountered: " + c);
                }
            };
            bitSet.set(i, value);
        }
        return new BitString(bitSet, text.length());
    }

    private final BitSet bitSet;
    private final int length;

    public BitString(BitSet bitSet, int length) {
        this.bitSet = bitSet;
        this.length = length;
    }

    public BitSet bitSet() {
        return bitSet;
    }

    public String asBitString() {
        var sb = new StringBuilder("B'");
        for (int i = 0; i < length; i++) {
            sb.append(bitSet.get(i) ? '1' : '0');
        }
        sb.append("'");
        return sb.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitBitString(this, context);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + bitSet.hashCode();
        result = prime * result + length;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BitString other = (BitString) obj;
        return bitSet.equals(other.bitSet) && length == other.length;
    }
}
