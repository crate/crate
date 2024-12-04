/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.lucene.index.SortedNumericDocValues;

import io.crate.exceptions.ArrayViaDocValuesUnsupportedException;
import io.crate.execution.engine.fetch.ReaderContext;

public abstract class NumericColumnReference<T> extends LuceneCollectorExpression<T> {

    private final String luceneField;

    private SortedNumericDocValues docValues;
    private int docId;

    protected NumericColumnReference(String luceneField) {
        this.luceneField = luceneField;
    }

    protected abstract T convert(long input);

    @Override
    public T value() {
        try {
            if (docValues.advanceExact(docId) == false) {
                return null;
            }
            if (docValues.docValueCount() > 1) {
                throw new ArrayViaDocValuesUnsupportedException(luceneField);
            }
            return convert(docValues.nextValue());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNextDocId(int doc) {
        this.docId = doc;
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        this.docValues = DocValues.getSortedNumeric(context.reader(), luceneField);
    }
}
