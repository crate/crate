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

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jetbrains.annotations.NotNull;

import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.StorageSupport;

public final class DynamicIndexer implements ValueIndexer<Object> {

    private final ReferenceIdent refIdent;
    private final Function<ColumnIdent, Reference> getRef;
    private final int position;
    private final boolean useOids;
    private DataType<?> type = null;
    private ValueIndexer<Object> indexer;

    public DynamicIndexer(ReferenceIdent refIdent,
                          int position,
                          Function<ColumnIdent, Reference> getRef,
                          boolean useOids) {
        this.refIdent = refIdent;
        this.getRef = getRef;
        this.position = position;
        this.useOids = useOids;
    }

    /**
     * Create a new Reference based on a dynamically detected type
     */
    public static Reference buildReference(ReferenceIdent refIdent, DataType<?> type, int position, long oid) {
        throwOnNestedArray(type);
        StorageSupport<?> storageSupport = type.storageSupport();
        if (storageSupport == null) {
            throw new IllegalArgumentException(
                "Cannot create columns of type " + type.getName() + " dynamically. " +
                    "Storage is not supported for this type");
        }
        return new SimpleReference(
            refIdent,
            RowGranularity.DOC,
            type,
            ColumnPolicy.DYNAMIC,
            IndexType.PLAIN,
            true,
            storageSupport.docValuesDefault(),
            position,
            oid,
            false,
            null
        );
    }

    @Override
    @SuppressWarnings("unchecked")
    public void collectSchemaUpdates(Object value,
                                     Consumer<? super Reference> onDynamicColumn,
                                     Synthetics synthetics) throws IOException {
        if (type == null) {
            type = guessType(value);
            Reference newColumn = buildReference(refIdent, type, position, COLUMN_OID_UNASSIGNED);
            StorageSupport<?> storageSupport = type.storageSupport();
            assert storageSupport != null; // will have already thrown in buildReference
            indexer = (ValueIndexer<Object>) storageSupport.valueIndexer(
                refIdent.tableIdent(),
                newColumn,
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
    public void indexValue(@NotNull Object value, IndexDocumentBuilder docBuilder) throws IOException {
        if (type == null) {
            // At the second phase of indexing type is not null in almost all cases
            // except the case with array of nulls
            type = guessType(value);
        }
        StorageSupport<?> storageSupport = type.storageSupport();
        if (storageSupport == null) {
            throw new IllegalArgumentException(
                "Cannot create columns of type " + type.getName() + " dynamically. " +
                    "Storage is not supported for this type");
        }
        Reference newColumn = buildReference(refIdent, type, position, COLUMN_OID_UNASSIGNED);
        if (indexer == null) {
            // Reuse indexer if phase 1 already created one.
            // Phase 1 mutates indexer.innerTypes on new columns creation.
            // Phase 2 must be aware of all mapping updates.
            // noinspection unchecked
            indexer = (ValueIndexer<Object>) storageSupport.valueIndexer(refIdent.tableIdent(), newColumn, getRef);
        }
        value = type.sanitizeValue(value);
        indexer.indexValue(value, docBuilder);
    }

    @Override
    public String storageIdentLeafName() {
        return useOids
            ? SourceParser.UNKNOWN_COLUMN_PREFIX + refIdent.columnIdent().leafName()
            : refIdent.columnIdent().leafName();
    }

    /**
     * <p>
     * Like {@link DataTypes#guessType(Object)} but prefers types with a higher precision.
     * For example a `Integer` results in a BIGINT type.
     * </p>
     * <p>
     * Users should sanitize the value using {@link DataType#sanitizeValue(Object)}
     * of the result type.
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
            return ArrayType.makeArray(DataTypes.upcast(innerType), dimensions);
        }
        return DataTypes.upcast(type);
    }

    /**
     * We don't support dynamically-created nested arrays as the MapperParser code
     * used when reading from the translog can't handle them.  So we also check
     * here that we're not trying to dynamically create one.
     */
    private static void throwOnNestedArray(DataType<?> type) {
        if (type instanceof ArrayType<?> at) {
            if (at.innerType() instanceof ArrayType<?>) {
                throw new IllegalArgumentException("Dynamic nested arrays are not supported");
            }
        }
    }
}
