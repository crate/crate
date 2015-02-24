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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.analyze.WhereClause;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.CollectContextService;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.operation.*;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.*;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.cache.recycler.PageCacheRecycler;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.fieldvisitor.FieldsVisitor;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.DefaultSearchContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchLocalRequest;
import org.elasticsearch.threadpool.ThreadPool;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * collect documents from ES shard, a lucene index
 */
public class LuceneDocCollector extends Collector implements CrateCollector, RowUpstream {

    public static class CollectorFieldsVisitor extends FieldsVisitor {

        final HashSet<String> requiredFields;
        private boolean required = false;

        public CollectorFieldsVisitor(int size) {
            requiredFields = new HashSet<>(size);
        }

        public boolean addField(String name) {
            required = true;
            return requiredFields.add(name);
        }

        public boolean required() {
            return required;
        }

        @Override
        public Status needsField(FieldInfo fieldInfo) throws IOException {
            if (SourceFieldMapper.NAME.equals(fieldInfo.name)) {
                return Status.YES;
            }
            return requiredFields.contains(fieldInfo.name) ? Status.YES : Status.NO;
        }

        public void required(boolean required) {
            this.required = required;
        }
    }

    private final SearchContext searchContext;
    private final RowDownstreamHandle downstream;
    private boolean visitorEnabled = false;
    private AtomicReader currentReader;
    private RamAccountingContext ramAccountingContext;

    private final CollectorFieldsVisitor fieldsVisitor;
    private final InputRow inputRow;
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private Scorer scorer;
    private final int jobSearchContextId;

    public LuceneDocCollector(final UUID jobId,
                              final ThreadPool threadPool,
                              ClusterService clusterService,
                              CollectContextService collectorContextService,
                              ShardId shardId,
                              final IndexService indexService,
                              final ScriptService scriptService,
                              final CacheRecycler cacheRecycler,
                              final PageCacheRecycler pageCacheRecycler,
                              final BigArrays bigArrays,
                              List<Input<?>> inputs,
                              List<LuceneCollectorExpression<?>> collectorExpressions,
                              final Functions functions,
                              final WhereClause whereClause,
                              RowDownstream downStreamProjector,
                              int jobSearchContextId) throws Exception {
        this.downstream = downStreamProjector.registerUpstream(this);
        final SearchShardTarget searchShardTarget = new SearchShardTarget(
                clusterService.localNode().id(), shardId.getIndex(), shardId.id());
        this.inputRow = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        this.fieldsVisitor = new CollectorFieldsVisitor(collectorExpressions.size());
        this.jobSearchContextId = jobSearchContextId;

        final IndexShard indexShard = indexService.shardSafe(shardId.id());
        final int searchContextId = Objects.hash(jobId, shardId);
        this.searchContext = collectorContextService.getOrCreateContext(
                jobId,
                searchContextId,
                new Function<IndexReader, SearchContext>() {

                    @Nullable
                    @Override
                    public SearchContext apply(@Nullable IndexReader indexReader) {
                        // TODO: handle IndexReader
                        ShardSearchLocalRequest searchRequest = new ShardSearchLocalRequest(
                                new String[] { Constants.DEFAULT_MAPPING_TYPE },
                                System.currentTimeMillis()
                        );
                        SearchContext localContext = new DefaultSearchContext(
                                searchContextId,
                                searchRequest,
                                searchShardTarget,
                                EngineSearcher.getSearcherWithRetry(indexShard, null), // TODO: use same searcher/reader for same jobId and searchContextId
                                indexService,
                                indexShard,
                                scriptService,
                                cacheRecycler,
                                pageCacheRecycler,
                                bigArrays,
                                threadPool.estimatedTimeInMillisCounter()
                        );
                        LuceneQueryBuilder builder = new LuceneQueryBuilder(functions);
                        LuceneQueryBuilder.Context ctx = builder.convert(whereClause, localContext, indexService.cache());
                        localContext.parsedQuery(new ParsedQuery(ctx.query(), ImmutableMap.<String, Filter>of()));
                        Float minScore = ctx.minScore();
                        if (minScore != null) {
                            localContext.minimumScore(minScore);
                        }
                        return localContext;
                    }
                }
        );
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
        this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new UnexpectedCollectionTerminatedException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                            ramAccountingContext.limit()));
        }
        if (searchContext.minimumScore() != null
                && scorer.score() < searchContext.minimumScore()) {
            return;
        }

        if (visitorEnabled) {
            fieldsVisitor.reset();
            currentReader.document(doc, fieldsVisitor);
        }
        for (LuceneCollectorExpression e : collectorExpressions) {
            e.setNextDocId(doc);
        }
        if (!downstream.setNextRow(inputRow)) {
            // no more rows required, we can stop here
            throw new CollectionAbortedException();
        }
    }

    @Override
    public void setNextReader(AtomicReaderContext context) throws IOException {
        this.currentReader = context.reader();
        for (LuceneCollectorExpression expr : collectorExpressions) {
            expr.setNextReader(context);
        }
    }

    @Override
    public boolean acceptsDocsOutOfOrder() {
        return true;
    }

    @Override
    public void doCollect(RamAccountingContext ramAccountingContext) throws Exception {
        this.ramAccountingContext = ramAccountingContext;
        // start collect
        CollectorContext collectorContext = new CollectorContext()
                .searchContext(searchContext)
                .visitor(fieldsVisitor)
                .jobSearchContextId(jobSearchContextId);
        for (LuceneCollectorExpression<?> collectorExpression : collectorExpressions) {
            collectorExpression.startCollect(collectorContext);
        }
        visitorEnabled = fieldsVisitor.required();
        SearchContext.setCurrent(searchContext);
        Query query = searchContext.query();
        if (query == null) {
            query = new MatchAllDocsQuery();
        }

        // do the lucene search
        try {
            searchContext.searcher().search(query, this);
            downstream.finish();
        } catch (CollectionAbortedException e) {
            // yeah, that's ok! :)
            downstream.finish();
        } catch (Exception e) {
            downstream.fail(e);
            throw e;
        } finally {
            searchContext.close();
            SearchContext.removeCurrent();
        }
    }
}
