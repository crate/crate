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
import java.math.BigInteger;
import java.math.MathContext;
import java.util.function.Function;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.jetbrains.annotations.NotNull;

import io.crate.execution.dml.IndexDocumentBuilder;
import io.crate.execution.dml.ValueIndexer;
import io.crate.expression.reference.doc.lucene.BinaryColumnReference;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.NumericColumnReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocSysColumns;

/**
 * Takes care of writing, reading and querying of values of type {@link NumericType} in Lucene.
 *
 * <ul>
 * <li>Values with <= 18 digits are stored as `long` with numeric
 * doc-values</li>
 * <li>
 * Values with > 18 digits are stored with binary doc-values with dimensions set
 * to support using {@link PointRangeQuery}
 * </li>
 * </p>
 *
 **/
public final class NumericStorage extends StorageSupport<BigDecimal> {

    public static final int COMPACT_PRECISION = 18;
    public static long COMPACT_MIN_VALUE = -999999999999999999L;
    public static long COMPACT_MAX_VALUE = 999999999999999999L;

    public NumericStorage(NumericType numericType) {
        super(true, true, NumericEqQuery.of(numericType));
    }

    @Override
    public ValueIndexer<? super BigDecimal> valueIndexer(RelationName table,
                                                         Reference ref,
                                                         Function<ColumnIdent, Reference> getRef) {
        DataType<?> type = ArrayType.unnest(ref.valueType());
        assert type instanceof NumericType
            : "ValueIndexer on NumericStorage can only be used for numeric types";
        NumericType numericType = (NumericType) type;
        Integer precision = numericType.numericPrecision();
        if (precision == null || numericType.scale() == null) {
            throw new UnsupportedOperationException(
                "NUMERIC type requires precision and scale to support storage");
        } else if (precision <= COMPACT_PRECISION) {
            return new CompactNumericIndexer(ref);
        } else {
            return new LargeNumericIndexer(ref, numericType, precision);
        }
    }

    private abstract static class BaseNumericIndexer implements ValueIndexer<BigDecimal> {

        protected final Reference ref;
        protected final String name;

        protected BaseNumericIndexer(Reference ref) {
            this.ref = ref;
            this.name = ref.storageIdent();
        }

        @Override
        public String storageIdentLeafName() {
            return ref.storageIdentLeafName();
        }
    }

    private static class CompactNumericIndexer extends BaseNumericIndexer {

        private CompactNumericIndexer(Reference ref) {
            super(ref);
        }

        @Override
        public void indexValue(@NotNull BigDecimal value, IndexDocumentBuilder docBuilder) throws IOException {
            BigInteger unscaled = value.unscaledValue();
            long longValue = unscaled.longValueExact();

            if (this.ref.indexType() != IndexType.NONE) {
                docBuilder.addField(new LongPoint(name, longValue));
            }
            if (ref.hasDocValues()) {
                docBuilder.addField(new SortedNumericDocValuesField(name, longValue));
            } else {
                docBuilder.addField(new StoredField(name, longValue));
                docBuilder.addField(new Field(
                    DocSysColumns.FieldNames.NAME,
                    name,
                    DocSysColumns.FieldNames.FIELD_TYPE));
            }

            docBuilder.translogWriter().writeValue(value);
        }
    }

    private static class LargeNumericIndexer extends BaseNumericIndexer {

        private final FieldType fieldType;
        private final int maxBytes;

        private LargeNumericIndexer(Reference ref, NumericType type, int precision) {
            super(ref);
            this.maxBytes = type.maxBytes();
            this.fieldType = new FieldType();
            this.fieldType.setDimensions(1, maxBytes);
            this.fieldType.freeze();
        }

        @Override
        public void indexValue(@NotNull BigDecimal value, IndexDocumentBuilder docBuilder) throws IOException {
            BigInteger unscaled = value.unscaledValue();
            byte[] bytes = new byte[maxBytes];
            NumericUtils.bigIntToSortableBytes(unscaled, maxBytes, bytes, 0);
            if (this.ref.indexType() != IndexType.NONE) {
                docBuilder.addField(new Field(name, bytes, fieldType));
            }
            if (ref.hasDocValues()) {
                docBuilder.addField(new SortedSetDocValuesField(name, new BytesRef(bytes)));
            } else {
                docBuilder.addField(new StoredField(name, new BytesRef(bytes)));
                docBuilder.addField(new Field(
                    DocSysColumns.FieldNames.NAME,
                    name,
                    DocSysColumns.FieldNames.FIELD_TYPE));
            }
            docBuilder.translogWriter().writeValue(value);
        }
    }

    public static LuceneCollectorExpression<BigDecimal> getCollectorExpression(String fqn, NumericType type) {
        final Integer precision = type.numericPrecision();
        final MathContext mathContext = type.mathContext();

        if (precision == null || precision > COMPACT_PRECISION) {
            return new BinaryColumnReference<BigDecimal>(fqn) {

                @Override
                protected BigDecimal convert(BytesRef input) {
                    var bigInt = NumericUtils.sortableBytesToBigInt(input.bytes, input.offset, input.length);
                    Integer scale = type.scale();
                    return new BigDecimal(bigInt, scale == null ? 0 : scale, mathContext);
                }
            };
        }

        return new NumericColumnReference<>(fqn) {

            @Override
            protected BigDecimal convert(long input) {
                BigInteger bigInt = BigInteger.valueOf(input);
                Integer scale = type.scale();
                return new BigDecimal(bigInt, scale == null ? 0 : scale, mathContext);
            }
        };
    }
}
