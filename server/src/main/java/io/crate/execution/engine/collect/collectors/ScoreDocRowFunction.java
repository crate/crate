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

package io.crate.execution.engine.collect.collectors;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.jetbrains.annotations.Nullable;

import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.InputRow;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.expression.reference.doc.lucene.OrderByCollectorExpression;

class ScoreDocRowFunction implements Function<ScoreDoc, Row> {

    private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
    private final IndexReader indexReader;
    private final LuceneCollectorExpression<?>[] expressions;
    private final DummyScorer scorer;
    private final InputRow inputRow;
    private final Runnable onScoreDoc;
    private final ReaderContext[] readerContexts;

    ScoreDocRowFunction(IndexReader indexReader,
                        List<? extends Input<?>> inputs,
                        Collection<? extends LuceneCollectorExpression<?>> expressions,
                        DummyScorer scorer,
                        Runnable onScoreDoc) {
        this.indexReader = indexReader;
        this.expressions = expressions.toArray(new LuceneCollectorExpression[0]);
        this.scorer = scorer;
        this.inputRow = new InputRow(inputs);
        this.onScoreDoc = onScoreDoc;
        for (LuceneCollectorExpression<?> expression : this.expressions) {
            if (expression instanceof OrderByCollectorExpression orderByExpr) {
                orderByCollectorExpressions.add(orderByExpr);
            }
        }
        List<LeafReaderContext> leaves = indexReader.leaves();
        readerContexts = new ReaderContext[leaves.size()];
        try {
            for (int i = 0; i < readerContexts.length; i++) {
                readerContexts[i] = new ReaderContext(leaves.get(i));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Nullable
    @Override
    public Row apply(@Nullable ScoreDoc input) {
        onScoreDoc.run();
        if (input == null) {
            return null;
        }
        FieldDoc fieldDoc = (FieldDoc) input;
        scorer.score(fieldDoc.score);
        for (int i = 0; i < orderByCollectorExpressions.size(); i++) {
            orderByCollectorExpressions.get(i).setNextFieldDoc(fieldDoc);
        }
        List<LeafReaderContext> leaves = indexReader.leaves();
        int readerIndex = ReaderUtil.subIndex(fieldDoc.doc, leaves);
        LeafReaderContext subReaderContext = leaves.get(readerIndex);
        var readerContext = readerContexts[readerIndex];
        int subDoc = fieldDoc.doc - subReaderContext.docBase;
        try {
            for (LuceneCollectorExpression<?> expression : expressions) {
                expression.setNextReader(readerContext);
                expression.setNextDocId(subDoc);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return inputRow;
    }
}
