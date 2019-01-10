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

package org.elasticsearch.search.aggregations.metrics.scripted;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorer;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.script.ScriptedMetricAggContexts;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class ScriptedMetricAggregator extends MetricsAggregator {

    private final ScriptedMetricAggContexts.MapScript.LeafFactory mapScript;
    private final ScriptedMetricAggContexts.CombineScript combineScript;
    private final Script reduceScript;
    private Object aggState;

    protected ScriptedMetricAggregator(String name, ScriptedMetricAggContexts.MapScript.LeafFactory mapScript, ScriptedMetricAggContexts.CombineScript combineScript,
                                       Script reduceScript, Object aggState, SearchContext context, Aggregator parent,
                                       List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData)
                                       throws IOException {
        super(name, context, parent, pipelineAggregators, metaData);
        this.aggState = aggState;
        this.mapScript = mapScript;
        this.combineScript = combineScript;
        this.reduceScript = reduceScript;
    }

    @Override
    public boolean needsScores() {
        return true; // TODO: how can we know if the script relies on scores?
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx,
            final LeafBucketCollector sub) throws IOException {
        final ScriptedMetricAggContexts.MapScript leafMapScript = mapScript.newInstance(ctx);
        return new LeafBucketCollectorBase(sub, leafMapScript) {
            @Override
            public void setScorer(Scorer scorer) throws IOException {
                leafMapScript.setScorer(scorer);
            }

            @Override
            public void collect(int doc, long bucket) throws IOException {
                assert bucket == 0 : bucket;

                leafMapScript.setDocument(doc);
                leafMapScript.execute();
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        Object aggregation;
        if (combineScript != null) {
            aggregation = combineScript.execute();
            CollectionUtils.ensureNoSelfReferences(aggregation);
        } else {
            aggregation = aggState;
        }
        return new InternalScriptedMetric(name, aggregation, reduceScript, pipelineAggregators(),
                metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalScriptedMetric(name, null, reduceScript, pipelineAggregators(), metaData());
    }

    @Override
    protected void doPostCollection() throws IOException {
        CollectionUtils.ensureNoSelfReferences(aggState);

        super.doPostCollection();
    }
}
