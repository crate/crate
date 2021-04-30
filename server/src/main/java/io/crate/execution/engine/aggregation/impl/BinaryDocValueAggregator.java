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

package io.crate.execution.engine.aggregation.impl;

import io.crate.breaker.RamAccounting;
import io.crate.execution.engine.aggregation.DocValueAggregator;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.function.Function;

public class BinaryDocValueAggregator<T> implements DocValueAggregator<T> {

    private final String columnName;
    private final Function<RamAccounting, T> stateInitializer;
    private final CheckedBiConsumer<SortedBinaryDocValues, T, IOException> docValuesConsumer;

    protected SortedBinaryDocValues values;

    public BinaryDocValueAggregator(String columnName,
                                    Function<RamAccounting, T> stateInitializer,
                                    CheckedBiConsumer<SortedBinaryDocValues, T, IOException> docValuesConsumer) {
        this.columnName = columnName;
        this.stateInitializer = stateInitializer;
        this.docValuesConsumer = docValuesConsumer;
    }

    @Override
    public T initialState(RamAccounting ramAccounting) {
        return stateInitializer.apply(ramAccounting);
    }

    @Override
    public void loadDocValues(LeafReader reader) throws IOException {
        values = FieldData.toString(DocValues.getSortedSet(reader, columnName));
    }

    @Override
    public void apply(RamAccounting ramAccounting, int doc, T state) throws IOException {
        if (values.advanceExact(doc) && values.docValueCount() == 1) {
            docValuesConsumer.accept(values, state);
        }
    }

    @Nullable
    @Override
    public Object partialResult(RamAccounting ramAccounting, T state) {
        return state;
    }
}
