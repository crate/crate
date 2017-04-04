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

package io.crate.operation.collect.collectors;

import com.google.common.base.Function;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.operation.InputRow;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

class ScoreDocRowFunction implements Function<ScoreDoc, Row> {

    private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
    private final IndexReader indexReader;
    private final Collection<? extends LuceneCollectorExpression<?>> expressions;
    private final DummyScorer scorer;
    private final InputRow inputRow;


    public ScoreDocRowFunction(IndexReader indexReader,
                               List<? extends Input<?>> inputs,
                               Collection<? extends LuceneCollectorExpression<?>> expressions,
                               DummyScorer scorer) {
        this.indexReader = indexReader;
        this.expressions = expressions;
        this.scorer = scorer;
        this.inputRow = new InputRow(inputs);
        addOrderByExpressions(expressions);
    }

    private void addOrderByExpressions(Collection<? extends LuceneCollectorExpression<?>> expressions) {
        for (LuceneCollectorExpression<?> expression : expressions) {
            if (expression instanceof OrderByCollectorExpression) {
                orderByCollectorExpressions.add((OrderByCollectorExpression) expression);
            }
        }
    }

    @Nullable
    @Override
    public Row apply(@Nullable ScoreDoc input) {
        if (input == null) {
            return null;
        }
        FieldDoc fieldDoc = (FieldDoc) input;
        scorer.score(fieldDoc.score);
        for (OrderByCollectorExpression orderByCollectorExpression : orderByCollectorExpressions) {
            orderByCollectorExpression.setNextFieldDoc(fieldDoc);
        }
        List<LeafReaderContext> leaves = indexReader.leaves();
        int readerIndex = ReaderUtil.subIndex(fieldDoc.doc, leaves);
        LeafReaderContext subReaderContext = leaves.get(readerIndex);
        int subDoc = fieldDoc.doc - subReaderContext.docBase;
        for (LuceneCollectorExpression<?> expression : expressions) {
            expression.setNextReader(subReaderContext);
            expression.setNextDocId(subDoc);
        }
        return inputRow;
    }
}
