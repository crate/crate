/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import io.crate.operation.Input;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.doc.CollectorContext;
import io.crate.operation.reference.doc.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.*;
import io.crate.Constants;
import io.crate.action.SQLXContentQueryParser;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.List;

/**
 * collect documents from ES shard, a lucene index
 */
public class LuceneDocCollector extends Collector implements CrateCollector {

    private final SearchContext searchContext;
    private final Projector downStream;
    private final List<Input<?>> topLevelInputs;
    private final List<LuceneCollectorExpression<?>> collectorExpressions;

    public LuceneDocCollector(ClusterService clusterService,
                              ShardId shardId,
                              IndexService indexService,
                              ScriptService scriptService,
                              CacheRecycler cacheRecycler,
                              PageCacheRecycler pageCacheRecycler,
                              SQLXContentQueryParser sqlxContentQueryParser,
                              List<Input<?>> inputs,
                              List<LuceneCollectorExpression<?>> collectorExpressions,
                              BytesReference querySource,
                              Projector downStreamProjector) throws Exception {
        this.downStream = downStreamProjector;

        SearchShardTarget searchShardTarget = new SearchShardTarget(clusterService.localNode().id(), shardId.getIndex(), shardId.id());

        this.topLevelInputs = inputs;
        this.collectorExpressions = collectorExpressions;

        ShardSearchRequest shardSearchRequest = new ShardSearchRequest();
        shardSearchRequest.types(new String[]{Constants.DEFAULT_MAPPING_TYPE});
        shardSearchRequest.source(querySource);
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        searchContext = new DefaultSearchContext(0, shardSearchRequest,
                searchShardTarget,
                indexShard.acquireSearcher("search"),
                indexService,
                indexShard,
                scriptService,
                cacheRecycler,
                pageCacheRecycler
        );
        sqlxContentQueryParser.parse(searchContext, querySource);
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {}

    @Override
    public void collect(int doc) throws IOException {
        Object[] newRow = new Object[topLevelInputs.size()];
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
        }
        int i = 0;
        for (Input<?> input : topLevelInputs) {
            newRow[i++] = input.value();
        }
        if (!downStream.setNextRow(newRow)) {
            // no more rows required, we can stop here
            throw new CollectionTerminatedException();
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void doCollect() throws Exception {
        // start collect
        CollectorContext collectorContext = new CollectorContext().searchContext(searchContext);
        for (LuceneCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        SearchContext.setCurrent(searchContext);
        Query query = searchContext.query();
        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        // do the lucene search
        try {
            searchContext.searcher().search(query, this);
        } finally {
            searchContext.release();
            SearchContext.removeCurrent();
        }
    }
}
