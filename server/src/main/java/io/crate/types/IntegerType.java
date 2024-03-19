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
import io.crate.execution.dml.IntIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.statistics.ColumnStatsSupport;

public class IntegerType extends DataType<Integer> implements Streamer<Integer>, FixedWidthType {

    public static final IntegerType INSTANCE = new IntegerType();
    public static final int ID = 9;
    public static final int PRECISION = 32;
    public static final int INTEGER_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(Integer.class);
    private static final StorageSupport<Number> STORAGE = new StorageSupport<>(true, true, new IntEqQuery()) {

        @Override
        public ValueIndexer<Number> valueIndexer(RelationName table,
                                                 Reference ref,
                                                 Function<String, FieldType> getFieldType,
                                                 Function<ColumnIdent, Reference> getRef) {
            return new IntIndexer(ref, getFieldType.apply(ref.storageIdent()));
        }
    };

    private IntegerType() {
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.INTEGER;
    }

    @Override
    public String getName() {
        return "integer";
    }

    @Override
    public Integer numericPrecision() {
        return PRECISION;
    }

    @Override
    public Streamer<Integer> streamer() {
        return this;
    }

    @Override
    public Integer implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        if (value == null) {
            return null;
        } else if (value instanceof Integer i) {
            return i;
        } else if (value instanceof String str) {
            return Integer.parseInt(str);
        } else if (value instanceof Regproc regproc) {
            return regproc.oid();
        } else if (value instanceof BigDecimal bigDecimalValue) {
            var max = BigDecimal.valueOf(Integer.MAX_VALUE).toBigInteger();
            var min = BigDecimal.valueOf(Integer.MIN_VALUE).toBigInteger();
            if (max.compareTo(bigDecimalValue.toBigInteger()) <= 0
                || min.compareTo(bigDecimalValue.toBigInteger()) >= 0) {
                throw new IllegalArgumentException(getName() + " value out of range: " + value);
            }
            return ((BigDecimal) value).intValue();
        } else if (value instanceof Number number) {
            long longVal = number.longValue();
            if (longVal < Integer.MIN_VALUE || Integer.MAX_VALUE < longVal) {
                throw new IllegalArgumentException("integer value out of range: " + longVal);
            }
            return ((Number) value).intValue();
        } else if (value instanceof Regclass regclass) {
            return regclass.oid();
        } else {
            throw new ClassCastException("Can't cast '" + value + "' to " + getName());
        }
    }

    @Override
    public Integer sanitizeValue(Object value) {
        if (value == null) {
            return null;
        } else if (value instanceof Integer i) {
            return i;
        } else {
            return ((Number) value).intValue();
        }
    }

    @Override
    public int compare(Integer val1, Integer val2) {
        return Integer.compare(val1, val2);
    }

    @Override
    public Integer readValueFrom(StreamInput in) throws IOException {
        return in.readBoolean() ? null : in.readInt();
    }

    @Override
    public void writeValueTo(StreamOutput out, Integer v) throws IOException {
        out.writeBoolean(v == null);
        if (v != null) {
            out.writeInt(v);
        }
    }

    @Override
    public int fixedSize() {
        return INTEGER_SIZE;
    }

    @Override
    public StorageSupport<Number> storageSupport() {
        return STORAGE;
    }

    @Override
    public ColumnStatsSupport<Integer> columnStatsSupport() {
        return ColumnStatsSupport.singleValued(Integer.class, IntegerType.this);
    }

    @Override
    public long valueBytes(Integer value) {
        return INTEGER_SIZE;
    }
}
