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
import java.util.Collection;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.common.xcontent.XContentBuilder;

import io.crate.execution.dml.Indexer.ColumnConstraint;
import io.crate.execution.dml.Indexer.Synthetic;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.ShortType;
import io.crate.types.StorageSupport;
import io.crate.types.UndefinedType;
import org.jetbrains.annotations.Nullable;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

public final class DynamicIndexer implements ValueIndexer<Object> {

    private final ReferenceIdent refIdent;
    private final Function<ColumnIdent, FieldType> getFieldType;
    private final Function<ColumnIdent, Reference> getRef;
    private final int position;
    private DataType<?> type = null;
    private ValueIndexer<Object> indexer;

    public DynamicIndexer(ReferenceIdent refIdent,
                          int position,
                          Function<ColumnIdent, FieldType> getFieldType,
                          Function<ColumnIdent, Reference> getRef) {
        this.refIdent = refIdent;
        this.getFieldType = getFieldType;
        this.getRef = getRef;
        this.position = position;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void collectSchemaUpdates(Object value,
                                     Consumer<? super Reference> onDynamicColumn,
                                     Map<ColumnIdent, Indexer.Synthetic> synthetics) throws IOException {
        if (type == null) {
            type = guessType(value);
            StorageSupport<?> storageSupport = type.storageSupport();
            if (storageSupport == null) {
                if (handleEmptyArray(type, value, null)) {
                    type = null; // guess type again with next value
                    return;
                }
                throw new IllegalArgumentException(
                    "Cannot create columns of type " + type.getName() + " dynamically. " +
                    "Storage is not supported for this type");
            }
            boolean nullable = true;
            Symbol defaultExpression = null;
            Reference newColumn = new SimpleReference(
                refIdent,
                RowGranularity.DOC,
                type,
                ColumnPolicy.DYNAMIC,
                IndexType.PLAIN,
                nullable,
                storageSupport.docValuesDefault(),
                position,
                COLUMN_OID_UNASSIGNED,
                false,
                defaultExpression
            );
            indexer = (ValueIndexer<Object>) storageSupport.valueIndexer(
                refIdent.tableIdent(),
                newColumn,
                getFieldType,
                getRef
            );
            onDynamicColumn.accept(newColumn);
        }
        value = type.sanitizeValue(value);
        indexer.collectSchemaUpdates(
            value,
            onDynamicColumn,
            synthetics
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public void indexValue(Object value,
                           XContentBuilder xcontentBuilder,
                           Consumer<? super IndexableField> addField,
                           Map<ColumnIdent, Synthetic> synthetics,
                           Map<ColumnIdent, ColumnConstraint> toValidate,
                           Function<Reference, String> columnKeyProvider) throws IOException {
        if (type == null) {
            // At the second phase of indexing type is not null in almost all cases
            // except the case with array of nulls
            type = guessType(value);
        }
        StorageSupport<?> storageSupport = type.storageSupport();
        if (storageSupport == null) {
            if (handleEmptyArray(type, value, xcontentBuilder)) {
                type = null; // guess type again with next value
                return;
            }
            throw new IllegalArgumentException(
                "Cannot create columns of type " + type.getName() + " dynamically. " +
                "Storage is not supported for this type");
        }
        boolean nullable = true;
        Symbol defaultExpression = null;
        Reference newColumn = new SimpleReference(
            refIdent,
            RowGranularity.DOC,
            type,
            ColumnPolicy.DYNAMIC,
            IndexType.PLAIN,
            nullable,
            storageSupport.docValuesDefault(),
            position,
            COLUMN_OID_UNASSIGNED,
            false,
            defaultExpression
        );
        if (indexer == null) {
            // Reuse indexer if phase 1 already created one.
            // Phase 1 mutates indexer.innerTypes on new columns creation.
            // Phase 2 must be aware of all mapping updates.
            indexer = (ValueIndexer<Object>) storageSupport.valueIndexer(
                refIdent.tableIdent(),
                newColumn,
                getFieldType,
                getRef
            );
        }
        value = type.sanitizeValue(value);
        indexer.indexValue(
            value,
            xcontentBuilder,
            addField,
            synthetics,
            toValidate,
            columnKeyProvider
        );
    }

    static boolean handleEmptyArray(DataType<?> type,
                                    Object value,
                                    @Nullable XContentBuilder builder) throws IOException {
        if (type instanceof ArrayType<?> && ArrayType.unnest(type) instanceof UndefinedType) {
            Collection<?> values = (Collection<?>) value;
            if (values.isEmpty() || values.stream().allMatch(x -> x == null)) {
                if (builder != null) {
                    builder.startArray();
                    for (int i = 0; i < values.size(); i++) {
                        builder.nullValue();
                    }
                    builder.endArray();
                }
                return true;
            }
        }
        return false;
    }


    /**
     * <p>
     * Like {@link DataTypes#guessType(Object)} but prefers types with a higher precision.
     * For example a `Integer` results in a BIGINT type.
     * </p>
     * <p>
     *  Users should sanitize the value using {@link DataType#sanitizeValue(Object)}
     *  of the result type.
     * </p>
     */
    static DataType<?> guessType(Object value) {
        DataType<?> type = DataTypes.guessType(value);
        if (type instanceof ArrayType<?>) {
            DataType<?> innerType = type;
            int dimensions = 0;
            while (innerType instanceof ArrayType<?> arrayType) {
                innerType = arrayType.innerType();
                dimensions++;
            }
            return ArrayType.makeArray(upcast(innerType), dimensions);
        }
        return upcast(type);
    }

    private static DataType<?> upcast(DataType<?> type) {
        return switch (type.id()) {
            case ByteType.ID, ShortType.ID, IntegerType.ID -> DataTypes.LONG;
            case FloatType.ID -> DataTypes.DOUBLE;
            default -> type;
        };
    }
}
