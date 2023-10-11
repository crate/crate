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
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.Streamer;
import io.crate.execution.dml.LongIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class LongType extends DataType<Long> implements FixedWidthType, Streamer<Long> {

    public static final LongType INSTANCE = new LongType();
    public static final int ID = 10;
    public static final int LONG_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Long.class);
    private static final StorageSupport<Long> STORAGE = new StorageSupport<>(
        true,
        true,
        new LongEqQuery()
    ) {

        @Override
        public ValueIndexer<Long> valueIndexer(RelationName table,
                                               Reference ref,
                                               Function<String, FieldType> getFieldType,
                                               Function<ColumnIdent, Reference> getRef) {
            return new LongIndexer(ref, getFieldType.apply(ref.storageIdent()));
        }
    };

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
        } else if (value instanceof Long l) {
            return l;
        } else if (value instanceof String str) {
            return Long.valueOf(str);
        } else if (value instanceof BigDecimal bigDecimalValue) {
            var max = BigDecimal.valueOf(Long.MAX_VALUE).toBigInteger();
            var min = BigDecimal.valueOf(Long.MIN_VALUE).toBigInteger();
            if (max.compareTo(bigDecimalValue.toBigInteger()) <= 0
                || min.compareTo(bigDecimalValue.toBigInteger()) >= 0) {
                throw new IllegalArgumentException(getName() + " value out of range: " + value);
            }
            return ((BigDecimal) value).longValue();
        } else if (value instanceof Number number) {
            return number.longValue();
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Long sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Long l) {
            return l;
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

    @Override
    public long valueBytes(Long value) {
        return LONG_SIZE;
    }
}
