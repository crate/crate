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

package org.apache.lucene.queries;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Objects;

public final class BinaryDocValuesRangeQuery extends Query {

    private final String fieldName;
    private final QueryType queryType;
    private final LengthType lengthType;
    private final BytesRef from;
    private final BytesRef to;
    private final Object originalFrom;
    private final Object originalTo;

    public BinaryDocValuesRangeQuery(String fieldName, QueryType queryType, LengthType lengthType,
                                     BytesRef from, BytesRef to,
                                     Object originalFrom, Object originalTo) {
        this.fieldName = fieldName;
        this.queryType = queryType;
        this.lengthType = lengthType;
        this.from = from;
        this.to = to;
        this.originalFrom = originalFrom;
        this.originalTo = originalTo;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        return new ConstantScoreWeight(this, boost) {

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                final BinaryDocValues values = context.reader().getBinaryDocValues(fieldName);
                if (values == null) {
                    return null;
                }

                final TwoPhaseIterator iterator = new TwoPhaseIterator(values) {

                    ByteArrayDataInput in = new ByteArrayDataInput();
                    BytesRef otherFrom = new BytesRef();
                    BytesRef otherTo = new BytesRef();

                    @Override
                    public boolean matches() throws IOException {
                        BytesRef encodedRanges = values.binaryValue();
                        in.reset(encodedRanges.bytes, encodedRanges.offset, encodedRanges.length);
                        int numRanges = in.readVInt();
                        final byte[] bytes = encodedRanges.bytes;
                        otherFrom.bytes = bytes;
                        otherTo.bytes = bytes;
                        int offset = in.getPosition();
                        for (int i = 0; i < numRanges; i++) {
                            int length = lengthType.readLength(bytes, offset);
                            otherFrom.offset = offset;
                            otherFrom.length = length;
                            offset += length;

                            length = lengthType.readLength(bytes, offset);
                            otherTo.offset = offset;
                            otherTo.length = length;
                            offset += length;

                            if (queryType.matches(from, to, otherFrom, otherTo)) {
                                return true;
                            }
                        }
                        assert offset == encodedRanges.offset + encodedRanges.length;
                        return false;
                    }

                    @Override
                    public float matchCost() {
                        return 4; // at most 4 comparisons
                    }
                };
                return new ConstantScoreScorer(this, score(), iterator);
            }

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return DocValues.isCacheable(ctx, fieldName);
            }
        };
    }

    @Override
    public String toString(String field) {
        return "BinaryDocValuesRangeQuery(fieldName=" + field + ",from=" + originalFrom + ",to=" + originalTo + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        BinaryDocValuesRangeQuery that = (BinaryDocValuesRangeQuery) o;
        return Objects.equals(fieldName, that.fieldName) &&
                queryType == that.queryType &&
                lengthType == that.lengthType &&
                Objects.equals(from, that.from) &&
                Objects.equals(to, that.to);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), fieldName, queryType, lengthType, from, to);
    }

    public enum QueryType {
        INTERSECTS {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                // part of the other range must touch this range
                // this:    |---------------|
                // other:               |------|
                return from.compareTo(otherTo) <= 0 && to.compareTo(otherFrom) >= 0;
            }
        }, WITHIN {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                // other range must entirely lie within this range
                // this:    |---------------|
                // other:       |------|
                return from.compareTo(otherFrom) <= 0 && to.compareTo(otherTo) >= 0;
            }
        }, CONTAINS {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                // this and other range must overlap
                // this:       |------|
                // other:    |---------------|
                return from.compareTo(otherFrom) >= 0 && to.compareTo(otherTo) <= 0;
            }
        }, CROSSES {
            @Override
            boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo) {
                // does not disjoint AND not within:
                return  (from.compareTo(otherTo) > 0 || to.compareTo(otherFrom) < 0) == false &&
                    (from.compareTo(otherFrom) <= 0 && to.compareTo(otherTo) >= 0) == false;
            }
        };

        abstract boolean matches(BytesRef from, BytesRef to, BytesRef otherFrom, BytesRef otherTo);

    }

    public enum LengthType {
        FIXED_4 {
            @Override
            int readLength(byte[] bytes, int offset) {
                return 4;
            }
        },
        FIXED_8 {
            @Override
            int readLength(byte[] bytes, int offset) {
                return 8;
            }
        },
        FIXED_16 {
            @Override
            int readLength(byte[] bytes, int offset) {
                return 16;
            }
        },
        VARIABLE {
            @Override
            int readLength(byte[] bytes, int offset) {
                // the first bit encodes the sign and the next 4 bits encode the number
                // of additional bytes
                int token = Byte.toUnsignedInt(bytes[offset]);
                int length = (token >>> 3) & 0x0f;
                if ((token & 0x80) == 0) {
                    length = 0x0f - length;
                }
                return 1 + length;
            }
        };

        /**
         * Return the length of the value that starts at {@code offset} in {@code bytes}.
         */
        abstract int readLength(byte[] bytes, int offset);
    }
}
