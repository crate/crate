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

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import io.crate.Constants;
import io.crate.action.sql.query.CrateSearchService;
import io.crate.analyze.OrderBy;
import io.crate.breaker.CrateCircuitBreakerService;
import io.crate.breaker.RamAccountingContext;
import io.crate.lucene.LuceneQueryBuilder;
import io.crate.metadata.Functions;
import io.crate.operation.*;
import io.crate.operation.reference.doc.lucene.CollectorContext;
import io.crate.operation.reference.doc.lucene.LuceneCollectorExpression;
import io.crate.operation.reference.doc.lucene.LuceneDocLevelReferenceResolver;
import io.crate.operation.reference.doc.lucene.OrderByCollectorExpression;
import io.crate.planner.node.dql.CollectNode;
import org.apache.lucene.index.*;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * collect documents from ES shard, a lucene index
 */
public class LuceneDocCollector extends Collector implements CrateCollector, RowUpstream {

    private final CollectorFieldsVisitor fieldsVisitor;
    private final InputRow inputRow;
    private boolean visitorEnabled = false;
    private AtomicReader currentReader;
    private RamAccountingContext ramAccountingContext;

    private final CollectInputSymbolVisitor<LuceneCollectorExpression<?>> inputSymbolVisitor;


    public final static int PAGE_SIZE = 10_000;

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
    private final List<LuceneCollectorExpression<?>> collectorExpressions;
    private final List<OrderByCollectorExpression> orderByCollectorExpressions = new ArrayList<>();
    private final Integer limit;
    private final OrderBy orderBy;

    public LuceneDocCollector(ThreadPool threadPool,
                              ClusterService clusterService,
                              LuceneQueryBuilder luceneQueryBuilder,
                              ShardId shardId,
                              IndexService indexService,
                              ScriptService scriptService,
                              CacheRecycler cacheRecycler,
                              PageCacheRecycler pageCacheRecycler,
                              BigArrays bigArrays,
                              List<Input<?>> inputs,
                              List<LuceneCollectorExpression<?>> collectorExpressions,
                              Functions functions,
                              CollectNode collectNode,
                              RowDownstream downStreamProjector) throws Exception {
        this.limit = collectNode.limit();
        this.orderBy = collectNode.orderBy();
        this.downstream = downStreamProjector.registerUpstream(this);
        SearchShardTarget searchShardTarget = new SearchShardTarget(
                clusterService.localNode().id(), shardId.getIndex(), shardId.id());
        this.inputRow = new InputRow(inputs);
        this.collectorExpressions = collectorExpressions;
        for (LuceneCollectorExpression expr : collectorExpressions) {
            if ( expr instanceof OrderByCollectorExpression) {
                orderByCollectorExpressions.add((OrderByCollectorExpression)expr);
            }
        }
        this.fieldsVisitor = new CollectorFieldsVisitor(collectorExpressions.size());

        ShardSearchLocalRequest searchRequest = new ShardSearchLocalRequest(
                new String[]{Constants.DEFAULT_MAPPING_TYPE},
                System.currentTimeMillis()
        );
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        searchContext = new DefaultSearchContext(0, searchRequest,
                searchShardTarget,
                EngineSearcher.getSearcherWithRetry(indexShard, null),
                indexService,
                indexShard,
                scriptService,
                cacheRecycler,
                pageCacheRecycler,
                bigArrays,
                threadPool.estimatedTimeInMillisCounter()
        );
        inputSymbolVisitor =
                new CollectInputSymbolVisitor<>(functions, LuceneDocLevelReferenceResolver.INSTANCE);
        try {
            LuceneQueryBuilder.Context ctx = luceneQueryBuilder.convert(collectNode.whereClause(), searchContext, indexService.cache());
            searchContext.parsedQuery(new ParsedQuery(ctx.query(), ImmutableMap.<String, Filter>of()));
            Float minScore = ctx.minScore();
            if (minScore != null) {
                searchContext.minimumScore(minScore);
            }
        } catch (Exception e) {
            searchContext.close();
            SearchContext.removeCurrent();
            throw e;
        }
    }

    @Override
    public void setScorer(Scorer scorer) throws IOException {
    }

    @Override
    public void collect(int doc) throws IOException {
        if (ramAccountingContext != null && ramAccountingContext.trippedBreaker()) {
            // stop collecting because breaker limit was reached
            throw new UnexpectedCollectionTerminatedException(
                    CrateCircuitBreakerService.breakingExceptionMessage(ramAccountingContext.contextId(),
                            ramAccountingContext.limit()));
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

    public void setNextOrderByValues(ScoreDoc scoreDoc) {
        for (OrderByCollectorExpression expr : orderByCollectorExpressions) {
            expr.setNextFieldDoc((FieldDoc)scoreDoc);
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
                .visitor(fieldsVisitor);
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
            if( orderBy != null) {
                Integer batchSize = MoreObjects.firstNonNull(limit, PAGE_SIZE);
                Sort sort = CrateSearchService.generateLuceneSort(searchContext, orderBy, inputSymbolVisitor);
                TopFieldDocs topFieldDocs = searchContext.searcher().search(query, batchSize, sort);
                ScoreDoc lastCollected = collectTopFields(topFieldDocs);
                if( limit == null) {
                    while (topFieldDocs.scoreDocs.length >= batchSize && lastCollected != null) {
                        topFieldDocs = (TopFieldDocs)searchContext.searcher().searchAfter(lastCollected, query, batchSize, sort);
                        lastCollected = collectTopFields(topFieldDocs);
                    }
                }
            } else {
                searchContext.searcher().search(query, this);
            }
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

    private ScoreDoc collectTopFields(TopFieldDocs topFieldDocs) throws IOException{
        IndexReaderContext indexReaderContext = searchContext.searcher().getTopReaderContext();
        ScoreDoc lastDoc = null;
        if(!indexReaderContext.leaves().isEmpty()) {
            for (ScoreDoc scoreDoc : topFieldDocs.scoreDocs) {
                int readerIndex = ReaderUtil.subIndex(scoreDoc.doc, searchContext.searcher().getIndexReader().leaves());
                AtomicReaderContext subReaderContext = searchContext.searcher().getIndexReader().leaves().get(readerIndex);
                int subDoc = scoreDoc.doc - subReaderContext.docBase;
                setNextReader(subReaderContext);
                setNextOrderByValues(scoreDoc);
                collect(subDoc);
                lastDoc = scoreDoc;
            }
        }
        return lastDoc;
    }
}
