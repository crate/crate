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

import io.crate.Streamer;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.math.BigDecimal;

public class ByteType extends DataType<Byte> implements Streamer<Byte>, FixedWidthType {

    public static final ByteType INSTANCE = new ByteType();
    public static final int ID = 2;
    private static final StorageSupport<Number> STORAGE = new StorageSupport<>(
        true, true, new IntEqQuery()
    );

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
        return "char";
    }

    @Override
    public Streamer<Byte> streamer() {
        return this;
    }

    @Override
    public Byte implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Byte) {
            return (Byte) value;
        } else if (value instanceof String) {
            return Byte.parseByte((String) value);
        } else if (value instanceof BigDecimal) {
            try {
                return ((BigDecimal) value).byteValueExact();
            } catch (ArithmeticException e) {
                throw new IllegalArgumentException("byte value out of range: " + value);
            }
        } else if (value instanceof Number) {
            int val = ((Number) value).intValue();
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
        } else if (value instanceof Byte) {
            return (Byte) value;
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
    public StorageSupport<Number> storageSupport() {
        return STORAGE;
    }
}
