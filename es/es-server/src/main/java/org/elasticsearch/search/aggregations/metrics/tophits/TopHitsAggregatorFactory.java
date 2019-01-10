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

package org.elasticsearch.search.aggregations.metrics.tophits;

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.fetch.StoredFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext;
import org.elasticsearch.search.fetch.subphase.DocValueFieldsContext.FieldAndFormat;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.ScriptFieldsContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.SubSearchContext;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TopHitsAggregatorFactory extends AggregatorFactory<TopHitsAggregatorFactory> {

    private final int from;
    private final int size;
    private final boolean explain;
    private final boolean version;
    private final boolean trackScores;
    private final Optional<SortAndFormats> sort;
    private final HighlightBuilder highlightBuilder;
    private final StoredFieldsContext storedFieldsContext;
    private final List<FieldAndFormat> docValueFields;
    private final List<ScriptFieldsContext.ScriptField> scriptFields;
    private final FetchSourceContext fetchSourceContext;

    TopHitsAggregatorFactory(String name, int from, int size, boolean explain, boolean version, boolean trackScores,
            Optional<SortAndFormats> sort, HighlightBuilder highlightBuilder, StoredFieldsContext storedFieldsContext,
            List<FieldAndFormat> docValueFields, List<ScriptFieldsContext.ScriptField> scriptFields, FetchSourceContext fetchSourceContext,
            SearchContext context, AggregatorFactory<?> parent, AggregatorFactories.Builder subFactories, Map<String, Object> metaData)
            throws IOException {
        super(name, context, parent, subFactories, metaData);
        this.from = from;
        this.size = size;
        this.explain = explain;
        this.version = version;
        this.trackScores = trackScores;
        this.sort = sort;
        this.highlightBuilder = highlightBuilder;
        this.storedFieldsContext = storedFieldsContext;
        this.docValueFields = docValueFields;
        this.scriptFields = scriptFields;
        this.fetchSourceContext = fetchSourceContext;
    }

    @Override
    public Aggregator createInternal(Aggregator parent, boolean collectsFromSingleBucket, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        SubSearchContext subSearchContext = new SubSearchContext(context);
        subSearchContext.parsedQuery(context.parsedQuery());
        subSearchContext.explain(explain);
        subSearchContext.version(version);
        subSearchContext.trackScores(trackScores);
        subSearchContext.from(from);
        subSearchContext.size(size);
        if (sort.isPresent()) {
            subSearchContext.sort(sort.get());
        }
        if (storedFieldsContext != null) {
            subSearchContext.storedFieldsContext(storedFieldsContext);
        }
        if (docValueFields != null) {
            subSearchContext.docValueFieldsContext(new DocValueFieldsContext(docValueFields));
        }
        for (ScriptFieldsContext.ScriptField field : scriptFields) {
            subSearchContext.scriptFields().add(field);
            }
        if (fetchSourceContext != null) {
            subSearchContext.fetchSourceContext(fetchSourceContext);
        }
        if (highlightBuilder != null) {
            subSearchContext.highlight(highlightBuilder.build(context.getQueryShardContext()));
        }
        return new TopHitsAggregator(context.fetchPhase(), subSearchContext, name, context, parent,
                pipelineAggregators, metaData);
    }

}
