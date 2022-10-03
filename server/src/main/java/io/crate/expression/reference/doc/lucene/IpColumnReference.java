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


import io.crate.exceptions.ArrayViaDocValuesUnsupportedException;
import io.crate.execution.engine.fetch.ReaderContext;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.DocValueFormat;

import java.io.IOException;
import java.io.UncheckedIOException;

public class IpColumnReference extends LuceneCollectorExpression<String> {

    private final String columnName;
    private SortedSetDocValues values;
    private int docId;

    public IpColumnReference(String columnName) {
        this.columnName = columnName;
    }

    @Override
    public String value() {
        try {
            if (values.advanceExact(docId)) {
                long ord = values.nextOrd();
                if (values.docValueCount() > 1) {
                    throw new ArrayViaDocValuesUnsupportedException(columnName);
                }
                BytesRef encoded = values.lookupOrd(ord);
                return (String) DocValueFormat.IP.format(encoded);
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void setNextDocId(int docId) {
        this.docId = docId;
    }

    @Override
    public void setNextReader(ReaderContext context) throws IOException {
        values = context.reader().getSortedSetDocValues(columnName);
        if (values == null) {
            values = DocValues.emptySortedSet();
        }
    }
}
