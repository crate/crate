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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics.stats;

import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.metrics.stats.ParsedStats;


public class ParsedStatsBucket extends ParsedStats implements StatsBucket {

    @Override
    public String getType() {
        return StatsBucketPipelineAggregationBuilder.NAME;
    }

    private static final ObjectParser<ParsedStatsBucket, Void> PARSER = new ObjectParser<>(
            ParsedStatsBucket.class.getSimpleName(), true, ParsedStatsBucket::new);

    static {
        declareStatsFields(PARSER);
    }

    public static ParsedStatsBucket fromXContent(XContentParser parser, final String name) {
        ParsedStatsBucket parsedStatsBucket = PARSER.apply(parser, null);
        parsedStatsBucket.setName(name);
        return parsedStatsBucket;
    }
}
