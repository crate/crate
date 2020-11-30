/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import java.math.BigInteger;

public class NumericType extends DataType<BigDecimal> implements Streamer<BigDecimal> {

    public static final NumericType INSTANCE = new NumericType();
    public static final int ID = 22;

    private NumericType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.NUMERIC;
    }

    @Override
    public String getName() {
        return "numeric";
    }

    @Override
    public Streamer<BigDecimal> streamer() {
        return this;
    }

    @Override
    public BigDecimal implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof String) {
            return new BigDecimal((String) value);
        } else if (value instanceof Long
                   || value instanceof Byte
                   || value instanceof Integer
                   || value instanceof Short) {
            return BigDecimal.valueOf(((Number) value).longValue());
        } else if (value instanceof Float || value instanceof Double) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public BigDecimal sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else {
            return (BigDecimal) value;
        }
    }

    @Override
    public int compare(BigDecimal o1, BigDecimal o2) {
        return o1.compareTo(o2);
    }

    @Override
    public BigDecimal readValueFrom(StreamInput in) throws IOException {
        byte[] bytes = new byte[in.readVInt()];
        in.readBytes(bytes, 0, bytes.length);
        return new BigDecimal(new BigInteger(bytes), 0);
    }

    @Override
    public void writeValueTo(StreamOutput out, BigDecimal v) throws IOException {
        var bytes = v.unscaledValue().toByteArray();
        out.writeVInt(bytes.length);
        out.writeBytes(bytes);
    }
}
