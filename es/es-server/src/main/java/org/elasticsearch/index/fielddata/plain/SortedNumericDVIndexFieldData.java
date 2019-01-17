/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.document.HalfFloatPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.AtomicNumericFieldData;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.IndexFieldData.XFieldComparatorSource.Nested;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.DoubleValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.FloatValuesComparatorSource;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * FieldData backed by {@link LeafReader#getSortedNumericDocValues(String)}
 * @see DocValuesType#SORTED_NUMERIC
 */
public class SortedNumericDVIndexFieldData extends DocValuesIndexFieldData implements IndexNumericFieldData {
    private final NumericType numericType;

    public SortedNumericDVIndexFieldData(Index index, String fieldNames, NumericType numericType) {
        super(index, fieldNames);
        if (numericType == null) {
            throw new IllegalArgumentException("numericType must be non-null");
        }
        this.numericType = numericType;
    }

    @Override
    public SortField sortField(Object missingValue, MultiValueMode sortMode, Nested nested, boolean reverse) {
        final XFieldComparatorSource source;
        switch (numericType) {
            case HALF_FLOAT:
            case FLOAT:
                source = new FloatValuesComparatorSource(this, missingValue, sortMode, nested);
                break;

            case DOUBLE:
                source = new DoubleValuesComparatorSource(this, missingValue, sortMode, nested);
                break;

            default:
                assert !numericType.isFloatingPoint();
                source = new LongValuesComparatorSource(this, missingValue, sortMode, nested);
                break;
        }

        /**
         * Check if we can use a simple {@link SortedNumericSortField} compatible with index sorting and
         * returns a custom sort field otherwise.
         */
        if (nested != null
                || (sortMode != MultiValueMode.MAX && sortMode != MultiValueMode.MIN)
                || numericType == NumericType.HALF_FLOAT) {
            return new SortField(fieldName, source, reverse);
        }

        final SortField sortField;
        final SortedNumericSelector.Type selectorType = sortMode == MultiValueMode.MAX ?
            SortedNumericSelector.Type.MAX : SortedNumericSelector.Type.MIN;
        switch (numericType) {
            case FLOAT:
                sortField = new SortedNumericSortField(fieldName, SortField.Type.FLOAT, reverse, selectorType);
                break;

            case DOUBLE:
                sortField = new SortedNumericSortField(fieldName, SortField.Type.DOUBLE, reverse, selectorType);
                break;

            default:
                assert !numericType.isFloatingPoint();
                sortField = new SortedNumericSortField(fieldName, SortField.Type.LONG, reverse, selectorType);
                break;
        }
        sortField.setMissingValue(source.missingObject(missingValue, reverse));
        return sortField;
    }

    @Override
    public NumericType getNumericType() {
        return numericType;
    }

    @Override
    public AtomicNumericFieldData loadDirect(LeafReaderContext context) throws Exception {
        return load(context);
    }

    @Override
    public AtomicNumericFieldData load(LeafReaderContext context) {
        final LeafReader reader = context.reader();
        final String field = fieldName;

        switch (numericType) {
            case HALF_FLOAT:
                return new SortedNumericHalfFloatFieldData(reader, field);
            case FLOAT:
                return new SortedNumericFloatFieldData(reader, field);
            case DOUBLE:
                return new SortedNumericDoubleFieldData(reader, field);
            default:
                return new SortedNumericLongFieldData(reader, field, numericType);
        }
    }

    /**
     * FieldData implementation for integral types.
     * <p>
     * Order of values within a document is consistent with
     * {@link Long#compareTo(Long)}.
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link DocValues#unwrapSingleton(SortedNumericDocValues)} will return
     * the underlying single-valued NumericDocValues representation.
     */
    static final class SortedNumericLongFieldData extends AtomicLongFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericLongFieldData(LeafReader reader, String field, NumericType numericType) {
            super(0L, numericType);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDocValues getLongValues() {
            try {
                return DocValues.getSortedNumeric(reader, field);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * FieldData implementation for 16-bit float values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Float#compareTo(Float)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 15) & 0x7fff}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     */
    static final class SortedNumericHalfFloatFieldData extends AtomicDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericHalfFloatFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);

                NumericDocValues single = DocValues.unwrapSingleton(raw);
                if (single != null) {
                    return FieldData.singleton(new SingleHalfFloatValues(single));
                } else {
                    return new MultiHalfFloatValues(raw);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Wraps a NumericDocValues and exposes a single 16-bit float per document.
     */
    static final class SingleHalfFloatValues extends NumericDoubleValues {
        final NumericDocValues in;

        SingleHalfFloatValues(NumericDocValues in) {
            this.in = in;
        }

        @Override
        public double doubleValue() throws IOException {
            return HalfFloatPoint.sortableShortToHalfFloat((short) in.longValue());
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return in.advanceExact(doc);
        }
    }

    /**
     * Wraps a SortedNumericDocValues and exposes multiple 16-bit floats per document.
     */
    static final class MultiHalfFloatValues extends SortedNumericDoubleValues {
        final SortedNumericDocValues in;

        MultiHalfFloatValues(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return HalfFloatPoint.sortableShortToHalfFloat((short) in.nextValue());
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }
    }

    /**
     * FieldData implementation for 32-bit float values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Float#compareTo(Float)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 31) & 0x7fffffff}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     */
    static final class SortedNumericFloatFieldData extends AtomicDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericFloatFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);

                NumericDocValues single = DocValues.unwrapSingleton(raw);
                if (single != null) {
                    return FieldData.singleton(new SingleFloatValues(single));
                } else {
                    return new MultiFloatValues(raw);
                }
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }

    /**
     * Wraps a NumericDocValues and exposes a single 32-bit float per document.
     */
    static final class SingleFloatValues extends NumericDoubleValues {
        final NumericDocValues in;

        SingleFloatValues(NumericDocValues in) {
            this.in = in;
        }

        @Override
        public double doubleValue() throws IOException {
            return NumericUtils.sortableIntToFloat((int) in.longValue());
        }

        @Override
        public boolean advanceExact(int doc) throws IOException {
            return in.advanceExact(doc);
        }
    }

    /**
     * Wraps a SortedNumericDocValues and exposes multiple 32-bit floats per document.
     */
    static final class MultiFloatValues extends SortedNumericDoubleValues {
        final SortedNumericDocValues in;

        MultiFloatValues(SortedNumericDocValues in) {
            this.in = in;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return in.advanceExact(target);
        }

        @Override
        public double nextValue() throws IOException {
            return NumericUtils.sortableIntToFloat((int) in.nextValue());
        }

        @Override
        public int docValueCount() {
            return in.docValueCount();
        }
    }

    /**
     * FieldData implementation for 64-bit double values.
     * <p>
     * Order of values within a document is consistent with
     * {@link Double#compareTo(Double)}, hence the following reversible
     * transformation is applied at both index and search:
     * {@code bits ^ (bits >> 63) & 0x7fffffffffffffffL}
     * <p>
     * Although the API is multi-valued, most codecs in Lucene specialize
     * for the case where documents have at most one value. In this case
     * {@link FieldData#unwrapSingleton(SortedNumericDoubleValues)} will return
     * the underlying single-valued NumericDoubleValues representation.
     */
    static final class SortedNumericDoubleFieldData extends AtomicDoubleFieldData {
        final LeafReader reader;
        final String field;

        SortedNumericDoubleFieldData(LeafReader reader, String field) {
            super(0L);
            this.reader = reader;
            this.field = field;
        }

        @Override
        public SortedNumericDoubleValues getDoubleValues() {
            try {
                SortedNumericDocValues raw = DocValues.getSortedNumeric(reader, field);
                return FieldData.sortableLongBitsToDoubles(raw);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot load doc values", e);
            }
        }

        @Override
        public Collection<Accountable> getChildResources() {
            return Collections.emptyList();
        }
    }
}
