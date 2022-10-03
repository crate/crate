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

package org.elasticsearch.index.fielddata;

import java.io.IOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

/**
 * Utility methods, similar to Lucene's {@link DocValues}.
 */
public enum FieldData {
    ;

    /**
     * Given a {@link SortedNumericDocValues}, return a {@link SortedNumericDoubleValues}
     * instance that will translate long values to doubles using
     * {@link org.apache.lucene.util.NumericUtils#sortableLongToDouble(long)}.
     */
    public static SortedNumericDoubleValues sortableLongBitsToDoubles(SortedNumericDocValues values) {
        final NumericDocValues singleton = DocValues.unwrapSingleton(values);
        if (singleton != null) {
            return new SingletonSortedNumericDoubleValues(new SortableLongBitsToNumericDoubleValues(singleton));
        } else {
            return new SortableLongBitsToSortedNumericDoubleValues(values);
        }
    }

    /**
     * Return a {@link String} representation of the provided values. That is
     * typically used for scripts or for the `map` execution mode of terms aggs.
     * NOTE: this is slow!
     */
    public static SortedBinaryDocValues toString(final SortedSetDocValues values) {
        return new SortedBinaryDocValues() {

            @Override
            public boolean advanceExact(int doc) throws IOException {
                return values.advanceExact(doc);
            }

            @Override
            public int docValueCount() {
                return values.docValueCount();
            }

            @Override
            public BytesRef nextValue() throws IOException {
                return values.lookupOrd(values.nextOrd());
            }
        };
    }
}
