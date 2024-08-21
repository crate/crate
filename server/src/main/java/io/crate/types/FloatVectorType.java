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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.execution.dml.FloatVectorIndexer;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;

public class FloatVectorType extends DataType<float[]> implements Streamer<float[]> {

    public static final int ID = 28;
    public static final String NAME = "float_vector";
    public static final FloatVectorType INSTANCE_ONE = new FloatVectorType(1);
    public static final VectorSimilarityFunction SIMILARITY_FUNC = VectorSimilarityFunction.EUCLIDEAN;
    public static final int MAX_DIMENSIONS = 2048;

    private static final EqQuery<float[]> EQ_QUERY = new EqQuery<>() {

        @Override
        public Query termQuery(String field, float[] value, boolean hasDocValues, boolean isIndexed) {
            return null;
        }

        @Override
        public Query rangeQuery(String field,
                                float[] lowerTerm,
                                float[] upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                boolean hasDocValues,
                                boolean isIndexed) {
            return null;
        }

        @Override
        public Query termsQuery(String field, List<float[]> nonNullValues, boolean hasDocValues, boolean isIndexed) {
            return null;
        }
    };

    private static final StorageSupport<? super float[]> STORAGE_SUPPORT = new StorageSupport<>(
            true,
            true,
            EQ_QUERY) {

        @Override
        public ValueIndexer<? super float[]> valueIndexer(RelationName table,
                                                          Reference ref,
                                                          Function<ColumnIdent, Reference> getRef) {
            return new FloatVectorIndexer(ref);
        }
    };

    private final int dimensions;

    public FloatVectorType(int dimensions) {
        this.dimensions = dimensions;
    }

    public FloatVectorType(StreamInput in) throws IOException {
        this.dimensions = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(dimensions);
    }

    @Override
    public int compare(float[] o1, float[] o2) {
        return Arrays.compare(o1, o2);
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
    public Streamer<float[]> streamer() {
        return this;
    }

    @Override
    public float[] sanitizeValue(Object obj) {
        // Value is stored as list in _raw/xcontent/json
        if (obj instanceof List<?> values) {
            float[] result = new float[values.size()];
            for (int i = 0; i < result.length; i++) {
                var value = values.get(i);
                if (value == null) {
                    throw new UnsupportedOperationException("null values are not allowed for " + NAME);
                } else {
                    result[i] = ((Number) value).floatValue();
                }
            }
            return result;
        } else if (obj instanceof byte[] bytes) {
            float[] floats = new float[bytes.length / Float.BYTES];
            ByteBuffer.wrap(bytes).asFloatBuffer().get(floats);
            return floats;
        }
        return (float[]) obj;
    }

    @Override
    public float[] implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return sanitizeValue(value);
    }

    @Override
    public long valueBytes(float @Nullable [] value) {
        if (value == null) {
            return RamUsageEstimator.NUM_BYTES_OBJECT_REF;
        }
        return RamUsageEstimator.sizeOf(value);
    }

    @Override
    public TypeSignature getTypeSignature() {
        if (dimensions == 1) {
            return new TypeSignature(NAME);
        }
        return new TypeSignature(getName(), List.of(TypeSignature.of(dimensions)));
    }

    @Override
    public List<DataType<?>> getTypeParameters() {
        return List.of(DataTypes.INTEGER);
    }

    @Override
    public Integer characterMaximumLength() {
        return dimensions;
    }

    @Override
    public ColumnType<Expression> toColumnType(ColumnPolicy columnPolicy,
                                               @Nullable Supplier<List<ColumnDefinition<Expression>>> convertChildColumn) {
        return new ColumnType<>(getName(), List.of(dimensions));
    }

    @Override
    public void addMappingOptions(Map<String, Object> mapping) {
        mapping.put("dimensions", dimensions);
    }

    @Override
    public float[] readValueFrom(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            return in.readFloatArray();
        } else {
            return null;
        }
    }

    @Override
    public void writeValueTo(StreamOutput out, float[] v) throws IOException {
        if (v == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeFloatArray(v);
        }
    }

    @Override
    public StorageSupport<? super float[]> storageSupport() {
        return STORAGE_SUPPORT;
    }
}
