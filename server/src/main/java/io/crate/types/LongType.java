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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.math.BigDecimal;

public class LongType extends DataType<Long> implements FixedWidthType, Streamer<Long> {

    public static final LongType INSTANCE = new LongType();
    public static final int ID = 10;
    public static final int LONG_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Long.class);
    private static final StorageSupport<Long> STORAGE = new StorageSupport<>(true, true, new LongEqQuery());

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.LONG;
    }

    @Override
    public String getName() {
        return "bigint";
    }

    @Override
    public Streamer<Long> streamer() {
        return this;
    }

    @Override
    public Long implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Long) {
            return (Long) value;
        } else if (value instanceof String) {
            return Long.valueOf((String) value);
        } else if (value instanceof BigDecimal) {
            var bigDecimalValue = (BigDecimal) value;
            var max = BigDecimal.valueOf(Long.MAX_VALUE).toBigInteger();
            var min = BigDecimal.valueOf(Long.MIN_VALUE).toBigInteger();
            if (max.compareTo(bigDecimalValue.toBigInteger()) <= 0
                || min.compareTo(bigDecimalValue.toBigInteger()) >= 0) {
                throw new IllegalArgumentException(getName() + " value out of range: " + value);
            }
            return ((BigDecimal) value).longValue();
        } else if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Long sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Long) {
            return (Long) value;
        } else {
            return ((Number) value).longValue();
        }
    }

    @Override
    public int compare(Long val1, Long val2) {
        return Long.compare(val1, val2);
    }

    @Override
    public Long readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readLong();
    }

    @Override
    public void writeValueTo(StreamOutput out, Long v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeLong(v);
        }
    }

    @Override
    public int fixedSize() {
        return LONG_SIZE;
    }

    @Override
    public StorageSupport<Long> storageSupport() {
        return STORAGE;
    }
}
