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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.avg.AvgBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.max.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.min.MinBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.percentile.PercentilesBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.StatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats.extended.ExtendedStatsBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketmetrics.sum.SumBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketscript.BucketScriptPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketselector.BucketSelectorPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.bucketsort.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.cumulativesum.CumulativeSumPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.derivative.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.movavg.MovAvgPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.movfn.MovFnPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.serialdiff.SerialDiffPipelineAggregationBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;

import java.util.List;
import java.util.Map;

public final class PipelineAggregatorBuilders {

    private PipelineAggregatorBuilders() {
    }

    public static DerivativePipelineAggregationBuilder derivative(String name, String bucketsPath) {
        return new DerivativePipelineAggregationBuilder(name, bucketsPath);
    }

    public static MaxBucketPipelineAggregationBuilder maxBucket(String name, String bucketsPath) {
        return new MaxBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static MinBucketPipelineAggregationBuilder minBucket(String name, String bucketsPath) {
        return new MinBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static AvgBucketPipelineAggregationBuilder avgBucket(String name, String bucketsPath) {
        return new AvgBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static SumBucketPipelineAggregationBuilder sumBucket(String name, String bucketsPath) {
        return new SumBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static StatsBucketPipelineAggregationBuilder statsBucket(String name, String bucketsPath) {
        return new StatsBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static ExtendedStatsBucketPipelineAggregationBuilder extendedStatsBucket(String name,
            String bucketsPath) {
        return new ExtendedStatsBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    public static PercentilesBucketPipelineAggregationBuilder percentilesBucket(String name,
            String bucketsPath) {
        return new PercentilesBucketPipelineAggregationBuilder(name, bucketsPath);
    }

    /**
     * @deprecated use {@link #movingFunction(String, Script, String, int)} instead
     */
    @Deprecated
    public static MovAvgPipelineAggregationBuilder movingAvg(String name, String bucketsPath) {
        return new MovAvgPipelineAggregationBuilder(name, bucketsPath);
    }

    public static BucketScriptPipelineAggregationBuilder bucketScript(String name,
            Map<String, String> bucketsPathsMap, Script script) {
        return new BucketScriptPipelineAggregationBuilder(name, bucketsPathsMap, script);
    }

    public static BucketScriptPipelineAggregationBuilder bucketScript(String name, Script script,
            String... bucketsPaths) {
        return new BucketScriptPipelineAggregationBuilder(name, script, bucketsPaths);
    }

    public static BucketSelectorPipelineAggregationBuilder bucketSelector(String name,
            Map<String, String> bucketsPathsMap, Script script) {
        return new BucketSelectorPipelineAggregationBuilder(name, bucketsPathsMap, script);
    }

    public static BucketSelectorPipelineAggregationBuilder bucketSelector(String name, Script script,
            String... bucketsPaths) {
        return new BucketSelectorPipelineAggregationBuilder(name, script, bucketsPaths);
    }

    public static BucketSortPipelineAggregationBuilder bucketSort(String name, List<FieldSortBuilder> sorts) {
        return new BucketSortPipelineAggregationBuilder(name, sorts);
    }

    public static CumulativeSumPipelineAggregationBuilder cumulativeSum(String name,
            String bucketsPath) {
        return new CumulativeSumPipelineAggregationBuilder(name, bucketsPath);
    }

    public static SerialDiffPipelineAggregationBuilder diff(String name, String bucketsPath) {
        return new SerialDiffPipelineAggregationBuilder(name, bucketsPath);
    }

    public static MovFnPipelineAggregationBuilder movingFunction(String name, Script script,
                                                                 String bucketsPaths, int window) {
        return new MovFnPipelineAggregationBuilder(name, bucketsPaths, script, window);
    }
}
