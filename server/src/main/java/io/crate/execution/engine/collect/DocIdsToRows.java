/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.collect;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import com.carrotsearch.hppc.IntArrayList;

import org.apache.lucene.index.LeafReaderContext;

import io.crate.data.Row;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;

public final class DocIdsToRows implements Function<IntArrayList, Iterator<Row>> {

    private final InputRow inputRow;
    private final List<? extends LuceneCollectorExpression<?>> expressions;

    private static class Iter implements Iterator<Row> {

        private final IntArrayList docIds;
        private final InputRow inputRow;
        private final List<? extends LuceneCollectorExpression<?>> expressions;
        int idx = 0;

        public Iter(IntArrayList docIds,
                    InputRow inputRow,
                    List<? extends LuceneCollectorExpression<?>> expressions) {
            this.docIds = docIds;
            this.inputRow = inputRow;
            this.expressions = expressions;
        }

        @Override
        public boolean hasNext() {
            return idx < docIds.size();
        }

        @Override
        public Row next() {
            int docId = docIds.buffer[idx];
            idx++;
            for (int i = 0; i < expressions.size(); i++) {
                expressions.get(i).setNextDocId(docId);
            }
            return inputRow;
        }
    }

    public DocIdsToRows(InputRow inputRow,
                        List<? extends LuceneCollectorExpression<?>> expressions) {
        this.inputRow = inputRow;
        this.expressions = expressions;
    }

    @Override
    public Iterator<Row> apply(IntArrayList docIds) {
        return new Iter(docIds, inputRow, expressions);
    }

    public void init(CollectorContext collectorContext, LeafReaderContext leaf) throws IOException {
        for (int i = 0; i < expressions.size(); i++) {
            expressions.get(i).startCollect(collectorContext);
            expressions.get(i).setNextReader(leaf);
        }
    }
}
