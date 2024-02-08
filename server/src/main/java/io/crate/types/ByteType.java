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
import java.math.BigDecimal;
import java.util.function.Function;

import org.apache.lucene.document.FieldType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.execution.dml.IntIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class ByteType extends DataType<Byte> implements Streamer<Byte>, FixedWidthType {

    public static final ByteType INSTANCE = new ByteType();
    public static final int ID = 2;
    public static final int PRECISION = 8;
    private static final StorageSupport<Number> STORAGE = new StorageSupport<>(
            true,
            true,
            new IntEqQuery()) {

        @Override
        public ValueIndexer<Number> valueIndexer(RelationName table,
                                                 Reference ref,
                                                 Function<String, FieldType> getFieldType,
                                                 Function<ColumnIdent, Reference> getRef) {
            return new IntIndexer(ref, getFieldType.apply(ref.storageIdent()));
        }
    };

    private ByteType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.BYTE;
    }

    @Override
    public String getName() {
        return "byte";
    }

    @Override
    public Integer numericPrecision() {
        return PRECISION;
    }

    @Override
    public Streamer<Byte> streamer() {
        return this;
    }

    @Override
    public Byte implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Byte b) {
            return b;
        } else if (value instanceof String str) {
            return Byte.parseByte(str);
        } else if (value instanceof BigDecimal bigDecimal) {
            try {
                return bigDecimal.byteValueExact();
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("byte value out of range: " + value);
            }
        } else if (value instanceof Number number) {
            int val = number.intValue();
            if (val < Byte.MIN_VALUE || Byte.MAX_VALUE < val) {
                throw new IllegalArgumentException("byte value out of range: " + val);
            }
            return (byte) val;
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Byte sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Byte b) {
            return b;
        } else {
            return ((Number) value).byteValue();
        }
    }

    @Override
    public int compare(Byte val1, Byte val2) {
        return Byte.compare(val1, val2);
    }

    @Override
    public Byte readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readByte();
    }

    @Override
    public void writeValueTo(StreamOutput out, Byte v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeByte(v);
        }
    }

    @Override
    public int fixedSize() {
        return 16; // object overhead + 1 byte + 7 byte padding
    }

    @Override
    public long valueBytes(Byte value) {
        return 16;
    }

    @Override
    public StorageSupport<Number> storageSupport() {
        return STORAGE;
    }
}
