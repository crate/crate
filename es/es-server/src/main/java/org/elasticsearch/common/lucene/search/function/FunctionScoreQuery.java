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

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.FilterScorer;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * A query that allows for a pluggable boost function / filter. If it matches
 * the filter, it will be boosted by the formula.
 */
public class FunctionScoreQuery extends Query {
    public static final float DEFAULT_MAX_BOOST = Float.MAX_VALUE;

    public static class FilterScoreFunction extends ScoreFunction {
        public final Query filter;
        public final ScoreFunction function;

        public FilterScoreFunction(Query filter, ScoreFunction function) {
            super(function.getDefaultScoreCombiner());
            this.filter = filter;
            this.function = function;
        }

        @Override
        public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
            return function.getLeafScoreFunction(ctx);
        }

        @Override
        public boolean needsScores() {
            return function.needsScores();
        }

        @Override
        protected boolean doEquals(ScoreFunction other) {
            if (getClass() != other.getClass()) {
                return false;
            }
            FilterScoreFunction that = (FilterScoreFunction) other;
            return Objects.equals(this.filter, that.filter) && Objects.equals(this.function, that.function);
        }

        @Override
        protected int doHashCode() {
            return Objects.hash(filter, function);
        }

        @Override
        protected ScoreFunction rewrite(IndexReader reader) throws IOException {
            Query newFilter = filter.rewrite(reader);
            if (newFilter == filter) {
                return this;
            }
            return new FilterScoreFunction(newFilter, function);
        }

        @Override
        public float getWeight() {
            return function.getWeight();
        }
    }

    public enum ScoreMode implements Writeable {
        FIRST, AVG, MAX, SUM, MIN, MULTIPLY;

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }

        public static ScoreMode readFromStream(StreamInput in) throws IOException {
            return in.readEnum(ScoreMode.class);
        }

        public static ScoreMode fromString(String scoreMode) {
            return valueOf(scoreMode.toUpperCase(Locale.ROOT));
        }
    }

    final Query subQuery;
    final ScoreFunction[] functions;
    final ScoreMode scoreMode;
    final float maxBoost;
    private final Float minScore;

    protected final CombineFunction combineFunction;

    /**
     * Creates a FunctionScoreQuery without function.
     * @param subQuery The query to match.
     * @param minScore The minimum score to consider a document.
     * @param maxBoost The maximum applicable boost.
     */
    public FunctionScoreQuery(Query subQuery, Float minScore, float maxBoost) {
        this(subQuery, ScoreMode.FIRST, new ScoreFunction[0], CombineFunction.MULTIPLY, minScore, maxBoost);
    }

    /**
     * Creates a FunctionScoreQuery with a single {@link ScoreFunction}
     * @param subQuery The query to match.
     * @param function The {@link ScoreFunction} to apply.
     */
    public FunctionScoreQuery(Query subQuery, ScoreFunction function) {
        this(subQuery, function, CombineFunction.MULTIPLY, null, DEFAULT_MAX_BOOST);
    }


    /**
     * Creates a FunctionScoreQuery with a single function
     * @param subQuery The query to match.
     * @param function The {@link ScoreFunction} to apply.
     * @param combineFunction Defines how the query and function score should be applied.
     * @param minScore The minimum score to consider a document.
     * @param maxBoost The maximum applicable boost.
     */
    public FunctionScoreQuery(Query subQuery, ScoreFunction function, CombineFunction combineFunction, Float minScore, float maxBoost) {
        this(subQuery, ScoreMode.FIRST, new ScoreFunction[] { function }, combineFunction, minScore, maxBoost);
    }

    /**
     * Creates a FunctionScoreQuery with multiple score functions
     * @param subQuery The query to match.
     * @param scoreMode Defines how the different score functions should be combined.
     * @param functions The {@link ScoreFunction}s to apply.
     * @param combineFunction Defines how the query and function score should be applied.
     * @param minScore The minimum score to consider a document.
     * @param maxBoost The maximum applicable boost.
     */
    public FunctionScoreQuery(Query subQuery, ScoreMode scoreMode, ScoreFunction[] functions,
                              CombineFunction combineFunction, Float minScore, float maxBoost) {
        if (Arrays.stream(functions).anyMatch(func -> func == null)) {
            throw new IllegalArgumentException("Score function should not be null");
        }
        this.subQuery = subQuery;
        this.scoreMode = scoreMode;
        this.functions = functions;
        this.maxBoost = maxBoost;
        this.combineFunction = combineFunction;
        this.minScore = minScore;
    }

    public Query getSubQuery() {
        return subQuery;
    }

    public ScoreFunction[] getFunctions() {
        return functions;
    }

    public Float getMinScore() {
        return minScore;
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        Query rewritten = super.rewrite(reader);
        if (rewritten != this) {
            return rewritten;
        }
        Query newQ = subQuery.rewrite(reader);
        ScoreFunction[] newFunctions = new ScoreFunction[functions.length];
        boolean needsRewrite = (newQ != subQuery);
        for (int i = 0; i < functions.length; i++) {
            newFunctions[i] = functions[i].rewrite(reader);
            needsRewrite |= (newFunctions[i] != functions[i]);
        }
        if (needsRewrite) {
            return new FunctionScoreQuery(newQ, scoreMode, newFunctions, combineFunction, minScore, maxBoost);
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
        if (needsScores == false && minScore == null) {
            return subQuery.createWeight(searcher, needsScores, boost);
        }

        boolean subQueryNeedsScores = combineFunction != CombineFunction.REPLACE;
        Weight[] filterWeights = new Weight[functions.length];
        for (int i = 0; i < functions.length; ++i) {
            subQueryNeedsScores |= functions[i].needsScores();
            if (functions[i] instanceof FilterScoreFunction) {
                Query filter = ((FilterScoreFunction) functions[i]).filter;
                filterWeights[i] = searcher.createNormalizedWeight(filter, false);
            }
        }
        Weight subQueryWeight = subQuery.createWeight(searcher, subQueryNeedsScores, boost);
        return new CustomBoostFactorWeight(this, subQueryWeight, filterWeights, subQueryNeedsScores);
    }

    class CustomBoostFactorWeight extends Weight {

        final Weight subQueryWeight;
        final Weight[] filterWeights;
        final boolean needsScores;

        CustomBoostFactorWeight(Query parent, Weight subQueryWeight, Weight[] filterWeights, boolean needsScores) throws IOException {
            super(parent);
            this.subQueryWeight = subQueryWeight;
            this.filterWeights = filterWeights;
            this.needsScores = needsScores;
        }

        @Override
        public void extractTerms(Set<Term> terms) {
            subQueryWeight.extractTerms(terms);
        }

        private FunctionFactorScorer functionScorer(LeafReaderContext context) throws IOException {
            Scorer subQueryScorer = subQueryWeight.scorer(context);
            if (subQueryScorer == null) {
                return null;
            }
            final LeafScoreFunction[] leafFunctions = new LeafScoreFunction[functions.length];
            final Bits[] docSets = new Bits[functions.length];
            for (int i = 0; i < functions.length; i++) {
                ScoreFunction function = functions[i];
                leafFunctions[i] = function.getLeafScoreFunction(context);
                if (filterWeights[i] != null) {
                    ScorerSupplier filterScorerSupplier = filterWeights[i].scorerSupplier(context);
                    docSets[i] = Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorerSupplier);
                } else {
                    docSets[i] = new Bits.MatchAllBits(context.reader().maxDoc());
                }
            }
            return new FunctionFactorScorer(this, subQueryScorer, scoreMode, functions, maxBoost, leafFunctions,
                docSets, combineFunction, needsScores);
        }

        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
            Scorer scorer = functionScorer(context);
            if (scorer != null && minScore != null) {
                scorer = new MinScoreScorer(this, scorer, minScore);
            }
            return scorer;
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {

            Explanation expl = subQueryWeight.explain(context, doc);
            if (!expl.isMatch()) {
                return expl;
            }
            boolean singleFunction = functions.length == 1 && functions[0] instanceof FilterScoreFunction == false;
            if (functions.length > 0) {
                // First: Gather explanations for all functions/filters
                List<Explanation> functionsExplanations = new ArrayList<>();
                for (int i = 0; i < functions.length; ++i) {
                    if (filterWeights[i] != null) {
                        final Bits docSet = Lucene.asSequentialAccessBits(
                                context.reader().maxDoc(), filterWeights[i].scorerSupplier(context));
                        if (docSet.get(doc) == false) {
                            continue;
                        }
                    }
                    ScoreFunction function = functions[i];
                    Explanation functionExplanation = function.getLeafScoreFunction(context).explainScore(doc, expl);
                    if (function instanceof FilterScoreFunction) {
                        double factor = functionExplanation.getValue();
                        float sc = (float) factor;
                        Query filterQuery = ((FilterScoreFunction) function).filter;
                        Explanation filterExplanation = Explanation.match(sc, "function score, product of:",
                            Explanation.match(1.0f, "match filter: " + filterQuery.toString()), functionExplanation);
                        functionsExplanations.add(filterExplanation);
                    } else {
                        functionsExplanations.add(functionExplanation);
                    }
                }
                final Explanation factorExplanation;
                if (functionsExplanations.size() == 0) {
                    // it is a little weird to add a match although no function matches but that is the way function_score behaves right now
                    factorExplanation = Explanation.match(1.0f, "No function matched", Collections.emptyList());
                } else if (singleFunction && functionsExplanations.size() == 1) {
                    factorExplanation = functionsExplanations.get(0);
                } else {
                    FunctionFactorScorer scorer = functionScorer(context);
                    int actualDoc = scorer.iterator().advance(doc);
                    assert (actualDoc == doc);
                    double score = scorer.computeScore(doc, expl.getValue());
                    factorExplanation = Explanation.match(
                        (float) score,
                        "function score, score mode [" + scoreMode.toString().toLowerCase(Locale.ROOT) + "]", functionsExplanations);
                }
                expl = combineFunction.explain(expl, factorExplanation, maxBoost);
            }
            if (minScore != null && minScore > expl.getValue()) {
                expl = Explanation.noMatch("Score value is too low, expected at least " + minScore + " but got " + expl.getValue(), expl);
            }
            return expl;
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            // If minScore is not null, then matches depend on statistics of the
            // top-level reader.
            return minScore == null;
        }
    }

    static class FunctionFactorScorer extends FilterScorer {
        private final ScoreFunction[] functions;
        private final ScoreMode scoreMode;
        private final LeafScoreFunction[] leafFunctions;
        private final Bits[] docSets;
        private final CombineFunction scoreCombiner;
        private final float maxBoost;
        private final boolean needsScores;

        private FunctionFactorScorer(CustomBoostFactorWeight w, Scorer scorer, ScoreMode scoreMode, ScoreFunction[] functions,
                                     float maxBoost, LeafScoreFunction[] leafFunctions, Bits[] docSets,
                                     CombineFunction scoreCombiner, boolean needsScores) throws IOException {
            super(scorer, w);
            this.scoreMode = scoreMode;
            this.functions = functions;
            this.leafFunctions = leafFunctions;
            this.docSets = docSets;
            this.scoreCombiner = scoreCombiner;
            this.maxBoost = maxBoost;
            this.needsScores = needsScores;
        }

        @Override
        public float score() throws IOException {
            int docId = docID();
            // Even if the weight is created with needsScores=false, it might
            // be costly to call score(), so we explicitly check if scores
            // are needed
            float subQueryScore = needsScores ? super.score() : 0f;
            if (leafFunctions.length == 0) {
                return subQueryScore;
            }
            double factor = computeScore(docId, subQueryScore);
            float finalScore = scoreCombiner.combine(subQueryScore, factor, maxBoost);
            if (finalScore == Float.NEGATIVE_INFINITY || Float.isNaN(finalScore)) {
                /*
                  These scores are invalid for score based {@link org.apache.lucene.search.TopDocsCollector}s.
                  See {@link org.apache.lucene.search.TopScoreDocCollector} for details.
                 */
                throw new ElasticsearchException("function score query returned an invalid score: " + finalScore + " for doc: " + docId);
            }
            return finalScore;
        }

        protected double computeScore(int docId, float subQueryScore) throws IOException {
            double factor = 1d;
            switch(scoreMode) {
                case FIRST:
                    for (int i = 0; i < leafFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            factor = leafFunctions[i].score(docId, subQueryScore);
                            break;
                        }
                    }
                    break;
                case MAX:
                    double maxFactor = Double.NEGATIVE_INFINITY;
                    for (int i = 0; i < leafFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            maxFactor = Math.max(leafFunctions[i].score(docId, subQueryScore), maxFactor);
                        }
                    }
                    if (maxFactor != Float.NEGATIVE_INFINITY) {
                        factor = maxFactor;
                    }
                    break;
                case MIN:
                    double minFactor = Double.POSITIVE_INFINITY;
                    for (int i = 0; i < leafFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            minFactor = Math.min(leafFunctions[i].score(docId, subQueryScore), minFactor);
                        }
                    }
                    if (minFactor != Float.POSITIVE_INFINITY) {
                        factor = minFactor;
                    }
                    break;
                case MULTIPLY:
                    for (int i = 0; i < leafFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            factor *= leafFunctions[i].score(docId, subQueryScore);
                        }
                    }
                    break;
                default: // Avg / Total
                    double totalFactor = 0.0f;
                    double weightSum = 0;
                    for (int i = 0; i < leafFunctions.length; i++) {
                        if (docSets[i].get(docId)) {
                            totalFactor += leafFunctions[i].score(docId, subQueryScore);
                            weightSum += functions[i].getWeight();
                        }
                    }
                    if (weightSum != 0) {
                        factor = totalFactor;
                        if (scoreMode == ScoreMode.AVG) {
                            factor /= weightSum;
                        }
                    }
                    break;
            }
            return factor;
        }
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder();
        sb.append("function score (").append(subQuery.toString(field)).append(", functions: [");
        for (ScoreFunction function : functions) {
            sb.append("{" + (function == null ? "" : function.toString()) + "}");
        }
        sb.append("])");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        FunctionScoreQuery other = (FunctionScoreQuery) o;
        return Objects.equals(this.subQuery, other.subQuery) && this.maxBoost == other.maxBoost &&
            Objects.equals(this.combineFunction, other.combineFunction) && Objects.equals(this.minScore, other.minScore) &&
            Objects.equals(this.scoreMode, other.scoreMode) &&
            Arrays.equals(this.functions, other.functions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), subQuery, maxBoost, combineFunction, minScore, scoreMode, Arrays.hashCode(functions));
    }
}
