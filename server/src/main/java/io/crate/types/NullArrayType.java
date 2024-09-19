/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.elasticsearch.Version;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.execution.dml.ArrayIndexer;
import io.crate.execution.dml.IndexDocumentBuilder;
import io.crate.execution.dml.ValueIndexer;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;

/**
 * Refers to arrays with either no value, or holding only nulls, such that it is
 * not possible to determine the type of the individual values in the array.
 * <p/>
 * NullArrayType columns will be automatically uplifted to a typed array when
 * a column value whose types can be determined is inserted into the table.
 */
public class NullArrayType extends DataType<List<Object>> {

    public static final NullArrayType INSTANCE = new NullArrayType();

    public static final String NAME = "array_of_null";
    public static final int ID = 101;

    @Override
    public int id() {
        return ID;
    }

    @Override
    public Precedence precedence() {
        return Precedence.ARRAY;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<List<Object>> streamer() {
        return new ArrayType.ArrayStreamer<>(UndefinedType.INSTANCE);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<Object> sanitizeValue(Object value) {
        return (List<Object>) value;
    }

    @Override
    public long valueBytes(@Nullable List<Object> value) {
        return Integer.BYTES;
    }

    @Override
    public int compare(List<Object> o1, List<Object> o2) {
        return Integer.compare(o1.size(), o2.size());
    }

    @Override
    public List<Object> implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return switch (value) {
            case Collection<?> c -> new ArrayList<>(c);
            case Map<?, ?> m -> {
                if (m.isEmpty()) {
                    yield List.of();
                }
                throw new IllegalArgumentException("Can't cast non-empty map to array of null");
            }
            case null -> List.of();
            default -> throw new IllegalArgumentException("Can't cast non-collection value to array of null");
        };
    }

    @Override
    public @Nullable StorageSupport<? super List<Object>> storageSupport() {
        return new StorageSupport<>(false, false, EqQuery.nonMatchingEqQuery()) {
            @Override
            public ValueIndexer<? super List<Object>> valueIndexer(RelationName table, Reference ref, Function<ColumnIdent, Reference> getRef) {
                return new ValueIndexer<>() {
                    @Override
                    public void indexValue(@NotNull List<Object> value, IndexDocumentBuilder docBuilder) {
                        docBuilder.translogWriter().writeNullArray(value.size());
                        if (docBuilder.getTableVersionCreated().onOrAfter(Version.V_5_9_0)) {
                            // map '[]' to '_array_length_ = 0'
                            // map '[null]' to '_array_length_ = 1'
                            // 'null' is not mapped; can utilize 'FieldExistsQuery' for 'IS NULL' filtering
                            docBuilder.addField(ArrayIndexer.arrayLengthField(ref, getRef, value.size()));
                        }
                    }

                    @Override
                    public String storageIdentLeafName() {
                        return ref.storageIdentLeafName();
                    }

                    @Override
                    @SuppressWarnings("unchecked")
                    public void collectSchemaUpdates(@Nullable List<Object> value, Consumer<? super Reference> onDynamicColumn, Synthetics synthetics) throws IOException {
                        if (value == null) {
                            return;
                        }
                        DataType<?> type = DataTypes.valueFromList(value, true);
                        if (type == NullArrayType.INSTANCE) {
                            return;
                        }
                        var storageSupport = type.storageSupport();
                        if (storageSupport == null) {
                            throw new IllegalArgumentException(
                                "Cannot create columns of type " + type.getName() + " dynamically. " +
                                    "Storage is not supported for this type");
                        }
                        Reference newColumn = new SimpleReference(
                            new ReferenceIdent(table, ref.column()),
                            RowGranularity.DOC,
                            type,
                            ref.columnPolicy(),
                            IndexType.PLAIN,
                            true,
                            storageSupport.docValuesDefault(),
                            ref.position(),
                            ref.oid(),
                            false,
                            null
                        );
                        onDynamicColumn.accept(newColumn);
                        var valueIndexer = (ValueIndexer<Object>) type.valueIndexer(table, newColumn, _ -> null);
                        valueIndexer.collectSchemaUpdates(value, onDynamicColumn, synthetics);
                    }
                };
            }
        };
    }
}
