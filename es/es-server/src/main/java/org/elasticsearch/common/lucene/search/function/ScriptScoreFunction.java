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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.script.ExplainableSearchScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;

public class ScriptScoreFunction extends ScoreFunction {

    static final class CannedScorer extends Scorer {
        protected int docid;
        protected float score;

        CannedScorer() {
            super(null);
        }

        @Override
        public int docID() {
            return docid;
        }

        @Override
        public float score() throws IOException {
            return score;
        }

        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }
    }

    private final Script sScript;

    private final ScoreScript.LeafFactory script;


    public ScriptScoreFunction(Script sScript, ScoreScript.LeafFactory script) {
        super(CombineFunction.REPLACE);
        this.sScript = sScript;
        this.script = script;
    }

    @Override
    public LeafScoreFunction getLeafScoreFunction(LeafReaderContext ctx) throws IOException {
        final ScoreScript leafScript = script.newInstance(ctx);
        final CannedScorer scorer = new CannedScorer();
        leafScript.setScorer(scorer);
        return new LeafScoreFunction() {
            @Override
            public double score(int docId, float subQueryScore) throws IOException {
                leafScript.setDocument(docId);
                scorer.docid = docId;
                scorer.score = subQueryScore;
                double result = leafScript.execute();
                return result;
            }

            @Override
            public Explanation explainScore(int docId, Explanation subQueryScore) throws IOException {
                Explanation exp;
                if (leafScript instanceof ExplainableSearchScript) {
                    leafScript.setDocument(docId);
                    scorer.docid = docId;
                    scorer.score = subQueryScore.getValue();
                    exp = ((ExplainableSearchScript) leafScript).explain(subQueryScore);
                } else {
                    double score = score(docId, subQueryScore.getValue());
                    String explanation = "script score function, computed with script:\"" + sScript + "\"";
                    if (sScript.getParams() != null) {
                        explanation += " and parameters: \n" + sScript.getParams().toString();
                    }
                    Explanation scoreExp = Explanation.match(
                            subQueryScore.getValue(), "_score: ",
                            subQueryScore);
                    return Explanation.match(
                            (float) score, explanation,
                            scoreExp);
                }
                return exp;
            }
        };
    }

    @Override
    public boolean needsScores() {
        return script.needs_score();
    }

    @Override
    public String toString() {
        return "script" + sScript.toString();
    }

    @Override
    protected boolean doEquals(ScoreFunction other) {
        ScriptScoreFunction scriptScoreFunction = (ScriptScoreFunction) other;
        return Objects.equals(this.sScript, scriptScoreFunction.sScript);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(sScript);
    }
}
