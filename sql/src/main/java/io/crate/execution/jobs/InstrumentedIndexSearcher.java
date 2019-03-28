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

package io.crate.execution.jobs;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.search.profile.Timer;
import org.elasticsearch.search.profile.query.ProfileWeight;
import org.elasticsearch.search.profile.query.QueryProfileBreakdown;
import org.elasticsearch.search.profile.query.QueryProfiler;
import org.elasticsearch.search.profile.query.QueryTimingType;

import java.io.IOException;

/**
 * {@link IndexSearcher} with the option to profile the queries.
 */
public class InstrumentedIndexSearcher extends IndexSearcher implements Releasable {

    private final QueryProfiler profiler;

    public InstrumentedIndexSearcher(IndexReader reader, QueryProfiler profiler) {
        super(reader);
        this.profiler = profiler;
    }

    @Override
    public Weight createWeight(Query query, ScoreMode scoreMode, float boost) throws IOException {
        QueryProfileBreakdown profile = profiler.getQueryBreakdown(query);
        Timer timer = profile.getTimer(QueryTimingType.CREATE_WEIGHT);
        timer.start();
        final Weight weight;
        try {
            weight = super.createWeight(query, scoreMode, boost);
        } finally {
            timer.stop();
            profiler.pollLastElement();
        }
        return new ProfileWeight(query, weight, profile);
    }


    @Override
    public void close() {
    }
}
