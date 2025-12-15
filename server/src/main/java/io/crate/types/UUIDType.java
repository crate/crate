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
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.PointInSetQuery;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.Nullable;

import io.crate.Streamer;
import io.crate.exceptions.ConversionException;
import io.crate.execution.dml.IndexDocumentBuilder;
import io.crate.execution.dml.ValueIndexer;
import io.crate.expression.reference.doc.lucene.BinaryColumnReference;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.SourceParser;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.RelationName;
import io.crate.metadata.ScopedRef;
import io.crate.metadata.doc.SysColumns;

public class UUIDType extends DataType<UUID> implements FixedWidthType, Streamer<UUID> {

    public static final int ID = 29;
    public static final String NAME = "uuid";
    public static final UUIDType INSTANCE = new UUIDType();

    // Num bytes including instance headers
    private static final int VALUE_BYTE_SIZE = (int) RamUsageEstimator.shallowSizeOfInstance(UUID.class);

    // storing two longs - mostSigBits and leastSigBits of UUID - as byte array
    private static final int STORAGE_BYTE_SIZE = 16;

    private static final StorageSupport<UUID> STORAGE = new StorageSupport<UUID>(true, true, new UUIDEqQuery()) {

        @Override
        public ValueIndexer<? super UUID> valueIndexer(RelationName table,
                                                       ScopedRef ref,
                                                       Function<ColumnIdent, ScopedRef> getRef) {
            return new UUIDIndexer(ref);
        }

        public UUID decode(ColumnIdent column,
                           SourceParser sourceParser,
                           Version tableVersion,
                           byte[] bytes) {

            return UUIDType.decode(bytes);
        }
    };

    private static UUID decode(byte[] bytes) {
        long mostSigBits = (long) BitUtil.VH_LE_LONG.get(bytes, 0);
        long leastSigBits = (long) BitUtil.VH_LE_LONG.get(bytes, 8);
        return new UUID(mostSigBits, leastSigBits);
    }

    private static byte[] encode(UUID value) {
        byte[] bytes = new byte[STORAGE_BYTE_SIZE];
        final long mostSigBits = value.getMostSignificantBits();
        final long leastSigBits = value.getLeastSignificantBits();
        BitUtil.VH_LE_LONG.set(bytes, 0, mostSigBits);
        BitUtil.VH_LE_LONG.set(bytes, 8, leastSigBits);
        return bytes;
    }

    @Override
    public int compare(UUID o1, UUID o2) {
        return o1.compareTo(o2);
    }

    @Override
    public UUID readValueFrom(StreamInput in) throws IOException {
        boolean hasValue = in.readBoolean();
        if (hasValue) {
            return in.readUUID();
        }
        return null;
    }

    @Override
    public void writeValueTo(StreamOutput out, UUID v) throws IOException {
        if (v == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeUUID(v);
        }
    }

    @Override
    public int id() {
        return ID;
    }

    @Override
    protected Precedence precedence() {
        return Precedence.UUID;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Streamer<UUID> streamer() {
        return this;
    }

    @Override
    public UUID sanitizeValue(Object value) {
        return switch (value) {
            case null -> null;
            case UUID uuid -> uuid;
            case String str -> UUID.fromString(str);
            default -> throw new ConversionException(value, this);
        };
    }

    @Override
    public UUID implicitCast(Object value) throws IllegalArgumentException, ClassCastException {
        return sanitizeValue(value);
    }

    @Override
    public long valueBytes(@Nullable UUID value) {
        return VALUE_BYTE_SIZE;
    }

    @Override
    public int fixedSize() {
        return VALUE_BYTE_SIZE;
    }

    @Override
    @Nullable
    public StorageSupport<? super UUID> storageSupport() {
        return STORAGE;
    }


    private static class UUIDEqQuery implements EqQuery<UUID> {

        @Override
        @Nullable
        public Query termQuery(String field, UUID value, boolean hasDocValues, boolean isIndexed) {
            return rangeQuery(
                field,
                value,
                value,
                true,
                true,
                hasDocValues,
                isIndexed
            );
        }

        @Override
        @Nullable
        public Query rangeQuery(String field,
                                UUID lowerTerm,
                                UUID upperTerm,
                                boolean includeLower,
                                boolean includeUpper,
                                boolean hasDocValues,
                                boolean isIndexed) {
            if (!isIndexed) {
                return null;
            }
            if (lowerTerm == null || upperTerm == null) {
                return null;
            }
            byte[] lower = encode(lowerTerm);
            byte[] upper = encode(upperTerm);
            return new PointRangeQuery(field, lower, upper, 1) {

                @Override
                protected String toString(int dimension, byte[] value) {
                    return decode(value).toString();
                }
            };
        }

        @Override
        @Nullable
        public Query termsQuery(String field,
                                List<UUID> nonNullValues,
                                boolean hasDocValues,
                                boolean isIndexed) {
            if (!isIndexed) {
                return null;
            }
            PointInSetQuery.Stream stream = new PointInSetQuery.Stream() {

                int idx = 0;

                @Override
                public BytesRef next() {
                    if (idx == nonNullValues.size()) {
                        return null;
                    }
                    UUID value = nonNullValues.get(idx);
                    idx++;
                    return new BytesRef(encode(value));
                }
            };
            return new PointInSetQuery(field, 1, STORAGE_BYTE_SIZE, stream) {

                @Override
                protected String toString(byte[] value) {
                    return decode(value).toString();
                }
            };
        }
    }

    private static class UUIDIndexer implements ValueIndexer<UUID> {

        private static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setDimensions(1, STORAGE_BYTE_SIZE);
            FIELD_TYPE.freeze();
        }

        private final ScopedRef ref;
        private final String name;

        public UUIDIndexer(ScopedRef ref) {
            this.ref = ref;
            this.name = ref.storageIdent();

        }

        @Override
        public void indexValue(UUID value, IndexDocumentBuilder docBuilder) throws IOException {
            byte[] bytes = encode(value);

            if (ref.indexType() != IndexType.NONE) {
                docBuilder.addField(new Field(name, bytes, FIELD_TYPE));
            }
            if (ref.hasDocValues()) {
                docBuilder.addField(new SortedSetDocValuesField(name, new BytesRef(bytes)));
            } else {
                if (docBuilder.maybeAddStoredField()) {
                    docBuilder.addField(new StoredField(name, new BytesRef(bytes)));
                }
                docBuilder.addField(new Field(
                    SysColumns.FieldNames.NAME,
                    name,
                    SysColumns.FieldNames.FIELD_TYPE));
            }
            docBuilder.translogWriter().writeValue(value.toString());
        }

        @Override
        public String storageIdentLeafName() {
            return ref.storageIdentLeafName();
        }
    }

    public static LuceneCollectorExpression<UUID> getCollectorExpression(String fqn) {
        return new BinaryColumnReference<UUID>(fqn) {

            @Override
            protected UUID convert(BytesRef input) {
                long mostSigBits = (long) BitUtil.VH_LE_LONG.get(input.bytes, input.offset);
                long leastSigBits = (long) BitUtil.VH_LE_LONG.get(input.bytes, input.offset + 8);
                return new UUID(mostSigBits, leastSigBits);
            }
        };
    }

    @Override
    public Sort sortSupport() {
        return Sort.COMPARATOR;
    }
}
