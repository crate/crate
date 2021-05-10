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

package io.crate.lucene.match;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.MultiTermQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.search.MatchQuery;

import javax.annotation.Nullable;

public class ParsedOptions {

    private final Float boost;
    private final String analyzer;
    private final MatchQuery.ZeroTermsQuery zeroTermsQuery;
    private final int maxExpansions;
    private final Fuzziness fuzziness;
    private final int prefixLength;
    private final boolean transpositions;

    private Float commonTermsCutoff;
    private BooleanClause.Occur operator = BooleanClause.Occur.SHOULD;
    private String minimumShouldMatch;
    private int phraseSlop = 0;
    private Float tieBreaker;
    private MultiTermQuery.RewriteMethod rewrite;

    public ParsedOptions(Float boost,
                         String analyzer,
                         MatchQuery.ZeroTermsQuery zeroTermsQuery,
                         int maxExpansions,
                         Fuzziness fuzziness,
                         int prefixLength,
                         boolean transpositions) {
        this.boost = boost;
        this.analyzer = analyzer;
        this.zeroTermsQuery = zeroTermsQuery;
        this.maxExpansions = maxExpansions;
        this.fuzziness = fuzziness;
        this.prefixLength = prefixLength;
        this.transpositions = transpositions;
    }

    @Nullable
    public Float boost() {
        return boost;
    }

    public void commonTermsCutoff(Float cutoff) {
        this.commonTermsCutoff = cutoff;
    }

    @Nullable
    public Float commonTermsCutoff() {
        return commonTermsCutoff;
    }

    public void operator(BooleanClause.Occur operator) {
        this.operator = operator;
    }

    public BooleanClause.Occur operator() {
        return operator;
    }

    public void minimumShouldMatch(String minimumShouldMatch) {
        this.minimumShouldMatch = minimumShouldMatch;
    }

    public void phraseSlop(int phraseSlop) {
        this.phraseSlop = phraseSlop;
    }

    public void tieBreaker(Float tieBreaker) {
        this.tieBreaker = tieBreaker;
    }

    @Nullable
    public Float tieBreaker() {
        return tieBreaker;
    }

    public void rewrite(MultiTermQuery.RewriteMethod rewrite) {
        this.rewrite = rewrite;
    }

    public int phraseSlop() {
        return phraseSlop;
    }

    public int maxExpansions() {
        return maxExpansions;
    }

    @Nullable
    public String minimumShouldMatch() {
        return minimumShouldMatch;
    }

    public MatchQuery.ZeroTermsQuery zeroTermsQuery() {
        return zeroTermsQuery;
    }

    @Nullable
    public String analyzer() {
        return analyzer;
    }

    @Nullable
    public Fuzziness fuzziness() {
        return fuzziness;
    }

    @Nullable
    public MultiTermQuery.RewriteMethod rewriteMethod() {
        return rewrite;
    }

    public int prefixLength() {
        return prefixLength;
    }

    public boolean transpositions() {
        return transpositions;
    }
}
