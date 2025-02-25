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

package io.crate.expression.reference.doc.lucene;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import io.crate.exceptions.ArrayViaDocValuesUnsupportedException;
import io.crate.execution.engine.fetch.ReaderContext;

public abstract class BinaryColumnReference<T> extends LuceneCollectorExpression<T> {

    private interface Loader<T> {
        T load(long ord) throws IOException;
    }

    private final String columnName;

    private Loader<T> loader;
    private SortedSetDocValues values;
    private int docId;

    public BinaryColumnReference(String columnName) {
        this.columnName = columnName;
    }

    protected abstract T convert(BytesRef input);

    @Override
    public final T value() throws ArrayViaDocValuesUnsupportedException {
        try {
            if (values.advanceExact(docId) == false) {
                return null;
            }
            if (values.docValueCount() > 1) {
                throw new ArrayViaDocValuesUnsupportedException(columnName);
            }
            return loader.load(values.nextOrd());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public final void setNextDocId(int docId) {
        this.docId = docId;
    }

    private static final int CACHE_SIZE = 256;
    private static final int CACHE_THRESHOLD = (int) (CACHE_SIZE * 1.5);

    @Override
    public final void setNextReader(ReaderContext context) throws IOException {
        values = DocValues.getSortedSet(context.reader(), columnName);
        if (values.getValueCount() > CACHE_THRESHOLD) {    // don't try and cache on high-cardinality fields
            loader = ord -> convert(values.lookupOrd(ord));
        } else {
            loader = new Loader<>() {

                final Cache<Long, T> cache = Caffeine.newBuilder().maximumSize(CACHE_SIZE).build();

                @Override
                public T load(long ord) throws IOException {
                    var value = cache.getIfPresent(ord);
                    if (value != null) {
                        // We can use `null` as a sentinel value here, because we will never
                        // store null values in a DV column.
                        return value;
                    }
                    value = convert(values.lookupOrd(values.nextOrd()));
                    cache.put(ord, value);
                    return value;
                }
            };
        }
    }
}

