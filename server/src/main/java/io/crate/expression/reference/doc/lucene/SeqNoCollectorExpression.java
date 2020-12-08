/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.reference.doc.lucene;

import io.crate.metadata.doc.DocSysColumns;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.StoredFieldVisitor;
import org.elasticsearch.common.CheckedBiConsumer;

import java.io.IOException;
import java.io.UncheckedIOException;

public class SeqNoCollectorExpression extends LuceneCollectorExpression<Long> {

    private NumericDocValues seqNumbers = null;
    private int doc;

    @Override
    public void setNextReader(LeafReaderContext context, CheckedBiConsumer<Integer, StoredFieldVisitor, IOException> fieldReader) throws IOException {
        try {
            seqNumbers = context.reader().getNumericDocValues(DocSysColumns.SEQ_NO.name());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setNextDocId(int doc) {
        this.doc = doc;
    }

    @Override
    public Long value() {
        try {
            if (seqNumbers != null && seqNumbers.advanceExact(doc)) {
                return seqNumbers.longValue();
            }
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
