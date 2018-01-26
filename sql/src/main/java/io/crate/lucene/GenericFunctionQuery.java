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

package io.crate.lucene;

import io.crate.expression.symbol.Function;
import io.crate.data.Input;
import io.crate.execution.engine.collect.collectors.CollectorFieldsVisitor;
import io.crate.expression.InputCondition;
import io.crate.expression.reference.doc.lucene.CollectorContext;
import io.crate.expression.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Query implementation which filters docIds by evaluating {@code condition} on each docId to verify if it matches.
 *
 * This query is very slow.
 */
class GenericFunctionQuery extends Query {

    private final Function function;
    private final LuceneCollectorExpression[] expressions;
    private final CollectorContext collectorContext;
    private final Input<Boolean> condition;

    GenericFunctionQuery(Function function,
                         Collection<? extends LuceneCollectorExpression<?>> expressions,
                         CollectorContext collectorContext,
                         Input<Boolean> condition) {
        this.function = function;
        // inner loop iterates over expressions - call toArray to avoid iterator allocations
        this.expressions = expressions.toArray(new LuceneCollectorExpression[0]);
        this.collectorContext = collectorContext;
        this.condition = condition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GenericFunctionQuery that = (GenericFunctionQuery) o;

        return function.equals(that.function);
    }

    @Override
    public int hashCode() {
        return function.hashCode();
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
        return new Weight(this) {
            @Override
            public void extractTerms(Set<Term> terms) {
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                final Scorer s = scorer(context);
                final boolean match;
                final TwoPhaseIterator twoPhase = s.twoPhaseIterator();
                if (twoPhase == null) {
                    match = s.iterator().advance(doc) == doc;
                } else {
                    match = twoPhase.approximation().advance(doc) == doc && twoPhase.matches();
                }
                if (match) {
                    assert s.score() == 0f : "score must be 0";
                    return Explanation.match(0f, "Match on id " + doc);
                } else {
                    return Explanation.match(0f, "No match on id " + doc);
                }
            }

            @Override
            public float getValueForNormalization() throws IOException {
                return 0;
            }

            @Override
            public void normalize(float norm, float boost) {
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                return new ConstantScoreScorer(this, 0f, getTwoPhaseIterator(context));
            }
        };
    }

    private FilteredTwoPhaseIterator getTwoPhaseIterator(final LeafReaderContext context) throws IOException {
        for (LuceneCollectorExpression expression : expressions) {
            expression.setNextReader(context);
        }
        return new FilteredTwoPhaseIterator(context.reader(), collectorContext.visitor(), condition, expressions);
    }

    @Override
    public String toString(String field) {
        return function.toString();
    }

    private static class FilteredTwoPhaseIterator extends TwoPhaseIterator {

        private final LeafReader reader;
        private final CollectorFieldsVisitor fieldsVisitor;
        private final Input<Boolean> condition;
        private final LuceneCollectorExpression[] expressions;
        private final boolean fieldsVisitorEnabled;

        FilteredTwoPhaseIterator(LeafReader reader,
                                 @Nullable CollectorFieldsVisitor fieldsVisitor,
                                 Input<Boolean> condition,
                                 LuceneCollectorExpression[] expressions) {
            super(DocIdSetIterator.all(reader.maxDoc()));
            this.reader = reader;
            this.fieldsVisitor = fieldsVisitor;
            this.fieldsVisitorEnabled = fieldsVisitor != null && fieldsVisitor.required();
            this.condition = condition;
            this.expressions = expressions;
        }

        @Override
        public boolean matches() throws IOException {
            int doc = approximation.docID();
            if (fieldsVisitorEnabled) {
                fieldsVisitor.reset();
                try {
                    reader.document(doc, fieldsVisitor);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            for (LuceneCollectorExpression expression : expressions) {
                expression.setNextDocId(doc);
            }
            return InputCondition.matches(condition);
        }

        @Override
        public float matchCost() {
            // Arbitrary number, we don't have a way to get the cost of the condition
            return 10;
        }
    }
}
