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

package io.crate.types;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.sql.tree.BitString;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;

public final class BitStringType extends DataType<BitString> implements Streamer<BitString>, FixedWidthType {

    public static final int ID = 25;
    public static final BitStringType INSTANCE_ONE = new BitStringType(1);
    public static final String NAME = "bit";
    public static final int DEFAULT_LENGTH = 1;
    private final int length;

    private static final StorageSupport<BitString> STORAGE = new StorageSupport<>(
        true,
        true,
        new EqQuery<BitString>() {

            @Override
            public Query termQuery(String field, BitString value) {
                return new TermQuery(new Term(field, new BytesRef(value.bitSet().toByteArray())));
            }

            @Override
            public Query rangeQuery(String field,
                                    BitString lowerTerm,
                                    BitString upperTerm,
                                    boolean includeLower,
                                    boolean includeUpper) {
                return null;
            }
        }
    );

    public BitStringType(StreamInput in) throws IOException {
        this.length = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(length);
    }

    public BitStringType(int length) {
        this.length = length;
    }

    /**
     * number of bits
     **/
    public int length() {
        return length;
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        return List.of(DataTypes.INTEGER);
    }

    @Override
    public TypeSignature getTypeSignature() {
        return new TypeSignature(getName(), List.of(TypeSignature.of(length)));
    }

    @Override
    public int compare(BitString o1, BitString o2) {
        return o1.compareTo(o2);
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.CUSTOM;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<BitString> streamer() {
        return this;
    }

    @Override
    public BitString sanitizeValue(Object value) {
        return (BitString) value;
    }

    @Override
    public BitString explicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        // explicit cast is allowed to trim or extend the bitstring
        if (value instanceof String str) {
            return BitString.ofRawBits(str, length);
        }
        BitString bs = (BitString) value;
        if (bs.length() == length) {
            return bs;
        } else {
            return new BitString(bs.bitSet().get(0, length), length);
        }
    }

    @Override
    public BitString implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        // implicit cast must not change the length of the value to have proper insert semantics
        // (wrong length = error)
        if (value instanceof String str) {
            return BitString.ofRawBits(str, str.length());
        }
        return (BitString) value;
    }

    @Override
    public BitString valueForInsert(Object value) {
        if (value == null) {
            return null;
        }
        BitString bitString = (BitString) value;
        if (bitString.length() == length) {
            return bitString;
        }
        throw new IllegalArgumentException(String.format(
            Locale.ENGLISH,
            "bit string length %d does not match type bit(%d)",
            bitString.length(),
            length
        ));
    }

    @Override
    public BitString readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return new BitString(BitSet.valueOf(in.readByteArray()), in.readVInt());
        } else {
            return null;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, BitString v) throws IOException {
        if (v == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeByteArray(v.bitSet().toByteArray());
            out.writeVInt(v.length());
        }
    }

    @Override
    public int fixedSize() {
        return (int) Math.floor(length / 8.0);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = super.hashCode();
        result = prime * result + length;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!super.equals(obj)) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BitStringType other = (BitStringType) obj;
        if (length != other.length) {
            return false;
        }
        return true;
    }

    @Override
    public ColumnType<Expression> toColumnType(ColumnPolicy columnPolicy,
                                               @Nullable Supplier<List<ColumnDefinition<Expression>>> convertChildColumn) {
        return new ColumnType<>(getName(), List.of(length));
    }

    @Override
    public StorageSupport<BitString> storageSupport() {
        return STORAGE;
    }
}
