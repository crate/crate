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

package io.crate.execution.dml;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.FloatVectorType;

public class FloatVectorIndexer implements ValueIndexer<float[]> {

    public static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setIndexOptions(IndexOptions.NONE);
        FIELD_TYPE.setStored(false);
        FIELD_TYPE.freeze();
    }

    final FieldType fieldType;
    private final String name;
    private final Reference ref;

    public FloatVectorIndexer(Reference ref) {
        this.fieldType = new FieldType(FIELD_TYPE);
        this.fieldType.setVectorAttributes(
            ref.valueType().characterMaximumLength(),
            VectorEncoding.FLOAT32,
            FloatVectorType.SIMILARITY_FUNC
        );
        this.ref = ref;
        this.name = ref.storageIdent();
    }

    @Override
    public float[] indexValue(float @Nullable @NotNull [] values,
                              Consumer<? super IndexableField> addField,
                              Synthetics synthetics,
                              Map<ColumnIdent, ColumnConstraint> toValidate) throws IOException {
        if (values == null) {
            return null;
        }

        createFields(
            name,
            fieldType,
            ref.indexType() != IndexType.NONE,
            ref.hasDocValues(),
            values,
            addField
        );
        if (fieldType.stored()) {
            throw new UnsupportedOperationException("Cannot store float_vector as stored field");
        }
        return values;
    }

    public static void createFields(String fqn,
                                    FieldType fieldType,
                                    boolean indexed,
                                    boolean hasDocValues,
                                    float @NotNull [] values,
                                    Consumer<? super IndexableField> addField) {
        if (indexed) {
            addField.accept(new KnnFloatVectorField(fqn, values, fieldType));
        }
        if (hasDocValues) {
            int capacity = values.length * Float.BYTES;
            ByteBuffer buffer = ByteBuffer.allocate(capacity).order(ByteOrder.BIG_ENDIAN);
            for (float value : values) {
                buffer.putFloat(value);
            }
            byte[] bytes = new byte[buffer.flip().limit()];
            buffer.get(bytes);
            var field = new BinaryDocValuesField(fqn, new BytesRef(bytes));
            addField.accept(field);
        } else {
            addField.accept(new Field(
                DocSysColumns.FieldNames.NAME,
                fqn,
                DocSysColumns.FieldNames.FIELD_TYPE));
        }
    }
}
