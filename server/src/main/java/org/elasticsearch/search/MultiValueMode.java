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


package org.elasticsearch.search;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;

import java.io.IOException;
import java.util.Locale;

/**
 * Defines what values to pick in the case a document contains multiple values for a particular field.
 */
public enum MultiValueMode {
    /**
     * Pick the sum of all the values.
     */
    SUM {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }

                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            return totalCount > 0 ? totalValue : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            final int count = values.docValueCount();
            double total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int totalCount = 0;
            double totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }

            return totalCount > 0 ? totalValue : missingValue;
        }
    },

    /**
     * Pick the average of all the values.
     */
    AVG {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            long total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return count > 1 ? Math.round((double) total / (double) count) : total;
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int totalCount = 0;
            long totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            if (totalCount < 1) {
                return missingValue;
            }
            return totalCount > 1 ? Math.round((double) totalValue / (double) totalCount) : totalValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            final int count = values.docValueCount();
            double total = 0;
            for (int index = 0; index < count; ++index) {
                total += values.nextValue();
            }
            return total / count;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int totalCount = 0;
            double totalValue = 0;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int index = 0; index < docCount; ++index) {
                        totalValue += values.nextValue();
                    }
                    totalCount += docCount;
                }
            }
            if (totalCount < 1) {
                return missingValue;
            }
            return totalValue / totalCount;
        }
    },

    /**
     * Pick the median of the values.
     */
    MEDIAN {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            int count = values.docValueCount();
            for (int i = 0; i < (count - 1) / 2; ++i) {
                values.nextValue();
            }
            if (count % 2 == 0) {
                return Math.round(((double) values.nextValue() + values.nextValue()) / 2);
            } else {
                return values.nextValue();
            }
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            int count = values.docValueCount();
            for (int i = 0; i < (count - 1) / 2; ++i) {
                values.nextValue();
            }
            if (count % 2 == 0) {
                return (values.nextValue() + values.nextValue()) / 2;
            } else {
                return values.nextValue();
            }
        }
    },

    /**
     * Pick the lowest value.
     */
    MIN {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            return values.nextValue();
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            boolean hasValue = false;
            long minValue = Long.MAX_VALUE;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    minValue = Math.min(minValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? minValue : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            return values.nextValue();
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            boolean hasValue = false;
            double minValue = Double.POSITIVE_INFINITY;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    minValue = Math.min(minValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? minValue : missingValue;
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values) throws IOException {
            return values.nextValue();
        }

        @Override
        protected BytesRef pick(BinaryDocValues values, BytesRefBuilder builder, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            BytesRefBuilder bytesRefBuilder = null;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final BytesRef innerValue = values.binaryValue();
                    if (bytesRefBuilder == null) {
                        builder.copyBytes(innerValue);
                        bytesRefBuilder = builder;
                    } else {
                        final BytesRef min = bytesRefBuilder.get().compareTo(innerValue) <= 0 ? bytesRefBuilder.get() : innerValue;
                        if (min == innerValue) {
                            bytesRefBuilder.copyBytes(min);
                        }
                    }
                }
            }
            return bytesRefBuilder == null ? null : bytesRefBuilder.get();
        }

        @Override
        protected int pick(SortedSetDocValues values) throws IOException {
            return Math.toIntExact(values.nextOrd());
        }

        @Override
        protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int ord = Integer.MAX_VALUE;
            boolean hasValue = false;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int innerOrd = values.ordValue();
                    ord = Math.min(ord, innerOrd);
                    hasValue = true;
                }
            }

            return hasValue ? ord : -1;
        }
    },

    /**
     * Pick the highest value.
     */
    MAX {
        @Override
        protected long pick(SortedNumericDocValues values) throws IOException {
            final int count = values.docValueCount();
            for (int i = 0; i < count - 1; ++i) {
                values.nextValue();
            }
            return values.nextValue();
        }

        @Override
        protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            boolean hasValue = false;
            long maxValue = Long.MIN_VALUE;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int i = 0; i < docCount - 1; ++i) {
                        values.nextValue();
                    }
                    maxValue = Math.max(maxValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? maxValue : missingValue;
        }

        @Override
        protected double pick(SortedNumericDoubleValues values) throws IOException {
            final int count = values.docValueCount();
            for (int i = 0; i < count - 1; ++i) {
                values.nextValue();
            }
            return values.nextValue();
        }

        @Override
        protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            boolean hasValue = false;
            double maxValue = Double.NEGATIVE_INFINITY;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final int docCount = values.docValueCount();
                    for (int i = 0; i < docCount - 1; ++i) {
                        values.nextValue();
                    }
                    maxValue = Math.max(maxValue, values.nextValue());
                    hasValue = true;
                }
            }
            return hasValue ? maxValue : missingValue;
        }

        @Override
        protected BytesRef pick(SortedBinaryDocValues values) throws IOException {
            int count = values.docValueCount();
            for (int i = 0; i < count - 1; ++i) {
                values.nextValue();
            }
            return values.nextValue();
        }

        @Override
        protected BytesRef pick(BinaryDocValues values, BytesRefBuilder builder, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            BytesRefBuilder bytesRefBuilder = null;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    final BytesRef innerValue = values.binaryValue();
                    if (bytesRefBuilder == null) {
                        builder.copyBytes(innerValue);
                        bytesRefBuilder = builder;
                    } else {
                        final BytesRef max = bytesRefBuilder.get().compareTo(innerValue) > 0 ? bytesRefBuilder.get() : innerValue;
                        if (max == innerValue) {
                            bytesRefBuilder.copyBytes(max);
                        }
                    }
                }
            }
            return bytesRefBuilder == null ? null : bytesRefBuilder.get();
        }

        @Override
        protected int pick(SortedSetDocValues values) throws IOException {
            long maxOrd = -1;
            for (int i = 0; i < values.docValueCount(); i++) {
                maxOrd = values.nextOrd();
            }
            return Math.toIntExact(maxOrd);
        }

        @Override
        protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
            int ord = -1;
            int count = 0;
            for (int doc = startDoc; doc < endDoc; doc = docItr.nextDoc()) {
                if (values.advanceExact(doc)) {
                    if (++count > maxChildren) {
                        break;
                    }
                    ord = Math.max(ord, values.ordValue());
                }
            }
            return ord;
        }
    };

    /**
     * A case insensitive version of {@link #valueOf(String)}
     *
     * @throws IllegalArgumentException if the given string doesn't match a sort mode or is <code>null</code>.
     */
    public static MultiValueMode fromString(String sortMode) {
        try {
            return valueOf(sortMode.toUpperCase(Locale.ROOT));
        } catch (Exception e) {
            throw new IllegalArgumentException("Illegal sort mode: " + sortMode);
        }
    }

    protected long pick(SortedNumericDocValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    protected long pick(SortedNumericDocValues values, long missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    protected double pick(SortedNumericDoubleValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    protected double pick(SortedNumericDoubleValues values, double missingValue, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    protected BytesRef pick(SortedBinaryDocValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    protected BytesRef pick(BinaryDocValues values, BytesRefBuilder builder, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    protected int pick(SortedSetDocValues values) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }

    protected int pick(SortedDocValues values, DocIdSetIterator docItr, int startDoc, int endDoc, int maxChildren) throws IOException {
        throw new IllegalArgumentException("Unsupported sort mode: " + this);
    }
}
