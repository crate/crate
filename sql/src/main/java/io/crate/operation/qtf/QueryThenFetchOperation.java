/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.qtf;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.FutureCallback;
import io.crate.Constants;
import io.crate.action.sql.query.CrateResultSorter;
import io.crate.action.sql.query.QueryShardRequest;
import io.crate.action.sql.query.QueryShardScrollRequest;
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.FailedShardsException;
import io.crate.executor.PageInfo;
import io.crate.executor.transport.task.elasticsearch.FieldExtractor;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.Reference;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchRequest;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.ScrollQuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryThenFetchOperation {

    private static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(5L);

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    private final TransportQueryShardAction transportQueryShardAction;
    private final SearchServiceTransportAction searchServiceTransportAction;
    private final SearchPhaseController searchPhaseController;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final CrateResultSorter crateResultSorter;
    private final ClusterService clusterService;

    /**
     * dummy request required to re-use the searchService transport
     */
    private final static SearchRequest EMPTY_SEARCH_REQUEST = new SearchRequest();
    private final static SearchScrollRequest EMPTY_SCROLL_REQUEST = new SearchScrollRequest();

    @Inject
    public QueryThenFetchOperation(ClusterService clusterService,
                                   TransportQueryShardAction transportQueryShardAction,
                                   SearchServiceTransportAction searchServiceTransportAction,
                                   SearchPhaseController searchPhaseController,
                                   ThreadPool threadPool,
                                   BigArrays bigArrays,
                                   CrateResultSorter crateResultSorter) {
        this.transportQueryShardAction = transportQueryShardAction;
        this.searchServiceTransportAction = searchServiceTransportAction;
        this.searchPhaseController = searchPhaseController;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.crateResultSorter = crateResultSorter;
        this.clusterService = clusterService;
    }

    public void execute(FutureCallback<QueryThenFetchContext> callback,
                        QueryThenFetchNode queryThenFetchNode,
                        List<Reference> outputs,
                        Optional<PageInfo> pageInfo) {

        // do stuff
        QueryThenFetchContext ctx = new QueryThenFetchContext(bigArrays, queryThenFetchNode, outputs, pageInfo);
        prepareRequests(ctx);

        if (!queryThenFetchNode.routing().hasLocations() || ctx.requests.size() == 0) {
            callback.onSuccess(ctx);
            return;
        }

        ClusterState state = clusterService.state();
        state.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        AtomicInteger totalOps = new AtomicInteger(0);

        int requestIdx = -1;
        for (Tuple<String, QueryShardRequest> requestTuple : ctx.requests) {
            requestIdx++;
            state.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, requestTuple.v2().index());
            transportQueryShardAction.executeQuery(
                    requestTuple.v1(),
                    requestTuple.v2(),
                    new QueryShardResponseListener(requestIdx, totalOps, ctx, callback)
            );
        }
    }

    private void prepareRequests(QueryThenFetchContext ctx) {
        ctx.requests = new ArrayList<>();
        Map<String, Map<String, Set<Integer>>> locations = ctx.searchNode.routing().locations();
        if (locations == null) {
            return;
        }

        int queryLimit;
        int queryOffset;
        // only set keepAlive on paged Requests
        Optional<TimeValue> keepAliveValue = Optional.absent();

        if (ctx.pageInfo.isPresent()) {
            // fetch all, including all offset stuff
            queryLimit = ctx.searchNode.offset() + Math.min(
                    ctx.searchNode.limitOr(Constants.DEFAULT_SELECT_LIMIT),
                    ctx.pageInfo.get().position() + ctx.pageInfo.get().size());
            queryOffset = 0;
            keepAliveValue = Optional.of(DEFAULT_KEEP_ALIVE);
        } else {
            assert ctx.searchNode.limitOr(0) >= 0 : "cannot handle NO_LIMIT at QTF";
            queryLimit = ctx.searchNode.limitOr(Constants.DEFAULT_SELECT_LIMIT);
            queryOffset = ctx.searchNode.offset();
        }


        for (Map.Entry<String, Map<String, Set<Integer>>> entry : locations.entrySet()) {
            String node = entry.getKey();
            for (Map.Entry<String, Set<Integer>> indexEntry : entry.getValue().entrySet()) {
                String index = indexEntry.getKey();
                Set<Integer> shards = indexEntry.getValue();

                for (Integer shard : shards) {
                    ctx.requests.add(new Tuple<>(
                            node,
                            new QueryShardRequest(
                                    index,
                                    shard,
                                    ctx.outputs,
                                    ctx.searchNode.orderBy(),
                                    ctx.searchNode.reverseFlags(),
                                    ctx.searchNode.nullsFirst(),
                                    queryLimit,
                                    queryOffset, // handle offset manually on handler for paged/scrolled calls
                                    ctx.searchNode.whereClause(),
                                    ctx.searchNode.partitionBy(),
                                    keepAliveValue
                            )
                    ));
                }
            }
        }
    }

    public void executePageQuery(
            final int from, final int size, final QueryThenFetchContext ctx, final FutureCallback<InternalSearchResponse> callback) {

        final QueryThenFetchPageContext pageContext = new QueryThenFetchPageContext(ctx);
        final Scroll scroll = new Scroll(DEFAULT_KEEP_ALIVE);

        int requestId = 0;
        for (final AtomicArray.Entry<QuerySearchResult> entry : ctx.queryResults.asList()) {

            DiscoveryNode node = ctx.nodes.get(entry.value.shardTarget().nodeId());
            final int currentId = requestId;
            QueryShardScrollRequest request = new QueryShardScrollRequest(entry.value.id(), scroll, from, size);
            transportQueryShardAction.executeScrollQuery(node.id(), request, new ActionListener<ScrollQuerySearchResult>() {
                @Override
                public void onResponse(ScrollQuerySearchResult scrollQuerySearchResult) {
                    final QuerySearchResult queryResult = scrollQuerySearchResult.queryResult();
                    pageContext.queryResults.set(currentId, queryResult);

                    if (pageContext.numOps.decrementAndGet() == 0) {
                        try {
                            executePageFetch(pageContext, callback);
                        } catch (Exception e) {
                            logger.error("error fetching page results for page from={} size={}", e, from ,size);
                            callback.onFailure(e);
                        }
                    } else if (logger.isTraceEnabled()) {
                        logger.trace("{} queries pending", pageContext.numOps.get());
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.error("error querying page results for page from={} size={}", t, from ,size);
                    callback.onFailure(t);
                }
            });
            requestId++;
        }
    }

    private void executePageFetch(final QueryThenFetchPageContext pageContext,
                                  final FutureCallback<InternalSearchResponse> callback) throws Exception {

        final ScoreDoc[] sortedShardList = searchPhaseController.sortDocs(true, pageContext.queryResults);
        AtomicArray<IntArrayList> docIdsToLoad = new AtomicArray<>(pageContext.queryResults.length());
        searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);

        if (docIdsToLoad.asList().isEmpty()) {
            callback.onSuccess(InternalSearchResponse.empty());
            return;
        }

        final ScoreDoc[] lastEmittedDocPerShard = searchPhaseController.getLastEmittedDocPerShard(sortedShardList, pageContext.queryResults.length());

        final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());

        for (final AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
            IntArrayList docIds = entry.value;
            final QuerySearchResult querySearchResult = pageContext.queryResults.get(entry.index);
            ScoreDoc lastEmittedDoc = lastEmittedDocPerShard[entry.index];
            ShardFetchRequest shardFetchRequest = new ShardFetchRequest(EMPTY_SCROLL_REQUEST, querySearchResult.id(), docIds, lastEmittedDoc);
            DiscoveryNode node = pageContext.queryThenFetchContext.nodes.get(querySearchResult.shardTarget().nodeId());
            searchServiceTransportAction.sendExecuteFetchScroll(node, shardFetchRequest, new SearchServiceListener<FetchSearchResult>() {
                @Override
                public void onResult(FetchSearchResult result) {
                    result.shardTarget(querySearchResult.shardTarget());
                    pageContext.fetchResults.set(entry.index, result);
                    if (counter.decrementAndGet() == 0) {
                        final InternalSearchResponse response = searchPhaseController.merge(
                                sortedShardList,
                                pageContext.queryResults,
                                pageContext.fetchResults);
                        callback.onSuccess(response);
                    } else {
                        logger.trace("{} fetch results left", counter.get());
                    }
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.debug("Failed to execute paged fetch phase", t);
                    pageContext.successfulFetchOps.decrementAndGet();
                    if (counter.decrementAndGet() == 0) {
                        callback.onFailure(t);
                    }
                }
            });
        }
    }

    private void moveToSecondPhase(QueryThenFetchContext ctx, FutureCallback<QueryThenFetchContext> callback) throws IOException {
        ScoreDoc[] lastEmittedDocs = null;
        if (ctx.pageInfo.isPresent()) {
            PageInfo pageInfo = ctx.pageInfo.get();

            int sortLimit = ctx.searchNode.offset() + pageInfo.size() + pageInfo.position();
            ctx.sortedShardList = crateResultSorter.sortDocs(ctx.queryResults, 0, sortLimit);
            lastEmittedDocs = searchPhaseController.getLastEmittedDocPerShard(
                    ctx.sortedShardList,
                    ctx.numShards);

            int fillOffset = pageInfo.position() + ctx.searchNode.offset();

            // create a fetchrequest for all documents even those hit by the offset
            // to set the lastemitteddoc on the shard
            crateResultSorter.fillDocIdsToLoad(
                    ctx.docIdsToLoad,
                    ctx.sortedShardList,
                    fillOffset
            );
        } else {
            ctx.sortedShardList = searchPhaseController.sortDocs(false, ctx.queryResults);
            searchPhaseController.fillDocIdsToLoad(
                    ctx.docIdsToLoad,
                    ctx.sortedShardList
            );
        }

        if (ctx.docIdsToLoad.asList().isEmpty()) {
            callback.onSuccess(ctx);
            return;
        }

        final AtomicInteger counter = new AtomicInteger(ctx.docIdsToLoad.asList().size());

        for (final AtomicArray.Entry<IntArrayList> entry : ctx.docIdsToLoad.asList()) {
            QuerySearchResult queryResult = ctx.queryResults.get(entry.index);
            DiscoveryNode node = ctx.nodes.get(queryResult.shardTarget().nodeId());
            ShardFetchSearchRequest fetchRequest = createFetchRequest(queryResult, entry, lastEmittedDocs);
            executeFetch(ctx, callback, entry.index, queryResult.shardTarget(), counter, fetchRequest, node);
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(QuerySearchResult queryResult,
                                                         AtomicArray.Entry<IntArrayList> entry, @Nullable ScoreDoc[] lastEmittedDocsPerShard) {
        if (lastEmittedDocsPerShard != null) {
            ScoreDoc lastEmittedDoc = lastEmittedDocsPerShard[entry.index];
            return new ShardFetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value, lastEmittedDoc);
        }
        return new ShardFetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value);
    }

    private void executeFetch(final QueryThenFetchContext ctx,
                              final FutureCallback<QueryThenFetchContext> callback,
                              final int shardIndex,
                              final SearchShardTarget shardTarget,
                              final AtomicInteger counter,
                              ShardFetchSearchRequest shardFetchSearchRequest,
                              DiscoveryNode node) {

        searchServiceTransportAction.sendExecuteFetch(
                node,
                shardFetchSearchRequest,
                new SearchServiceListener<FetchSearchResult>() {
                    @Override
                    public void onResult(FetchSearchResult result) {
                        result.shardTarget(shardTarget);
                        ctx.fetchResults.set(shardIndex, result);
                        if (counter.decrementAndGet() == 0) {
                            callback.onSuccess(ctx);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        ctx.docIdsToLoad.set(shardIndex, null);
                        ctx.addShardFailure(shardIndex, shardTarget, t);
                        if (counter.decrementAndGet() == 0) {
                            callback.onSuccess(ctx);
                        }
                    }
                }
        );
    }

    private void raiseEarlyFailure(QueryThenFetchContext ctx, FutureCallback<?> callback, Throwable t) {
        ctx.releaseAllContexts();
        t = Exceptions.unwrap(t);
        if (t instanceof QueryPhaseExecutionException) {
            callback.onFailure(t.getCause());
            return;
        }
        callback.onFailure(t);
    }

    public class QueryThenFetchPageContext {
        private final QueryThenFetchContext queryThenFetchContext;
        private final AtomicArray<QuerySearchResult> queryResults;
        private final AtomicArray<FetchSearchResult> fetchResults;
        private final AtomicInteger numOps;
        private final AtomicInteger successfulFetchOps;

        public QueryThenFetchPageContext(QueryThenFetchContext queryThenFetchContext) {
            this.queryThenFetchContext = queryThenFetchContext;
            this.queryResults = new AtomicArray<>(queryThenFetchContext.numShards());
            this.fetchResults = new AtomicArray<>(queryThenFetchContext.numShards());
            this.numOps = new AtomicInteger(queryThenFetchContext.numShards());
            this.successfulFetchOps = new AtomicInteger(queryThenFetchContext.numShards());
        }
    }

    public class QueryThenFetchContext implements Closeable {

        private final Optional<PageInfo> pageInfo;
        private final DiscoveryNodes nodes;
        private final QueryThenFetchNode searchNode;
        private final List<Reference> outputs;
        private final int numShards;
        private final int numColumns;
        private final BigArrays bigArrays;

        private final Map<SearchShardTarget, Long> searchContextIds;

        private final AtomicArray<IntArrayList> docIdsToLoad;
        private final AtomicArray<QuerySearchResult> queryResults;
        private final AtomicArray<FetchSearchResult> fetchResults;

        private final Object shardFailuresMutex = new Object();

        private volatile ScoreDoc[] sortedShardList;
        private volatile AtomicArray<ShardSearchFailure> shardFailures;
        private List<Tuple<String, QueryShardRequest>> requests;

        public QueryThenFetchContext(BigArrays bigArrays,
                                     QueryThenFetchNode node,
                                     List<Reference> outputs,
                                     Optional<PageInfo> pageInfo) {
            this.searchNode = node;
            this.outputs = outputs;
            this.pageInfo = pageInfo;
            this.numShards = node.routing().numShards();
            this.nodes = clusterService.state().nodes();
            this.bigArrays = bigArrays;

            searchContextIds = new ConcurrentHashMap<>();
            docIdsToLoad = new AtomicArray<>(numShards);
            queryResults = new AtomicArray<>(numShards);
            fetchResults = new AtomicArray<>(numShards);
            numColumns = node.outputs().size();
        }

        public void createSearchResponse(final FutureCallback<InternalSearchResponse> callback) {
            try {
                threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if(shardFailures != null && shardFailures.length() > 0){
                                FailedShardsException ex = new FailedShardsException(shardFailures.toArray(
                                        new ShardSearchFailure[shardFailures.length()]));
                                callback.onFailure(ex);
                                return;
                            }
                            ScoreDoc[] appliedSortedShardList = sortedShardList;
                            if (pageInfo.isPresent()) {
                                int offset = pageInfo.get().position();
                                appliedSortedShardList = Arrays.copyOfRange(
                                        sortedShardList,
                                        Math.min(offset, sortedShardList.length),
                                        sortedShardList.length
                                );
                            }
                            callback.onSuccess(searchPhaseController.merge(appliedSortedShardList, queryResults, fetchResults));
                        } catch (Throwable t) {
                            callback.onFailure(t);
                        } finally {
                            releaseIrrelevantSearchContexts(queryResults, docIdsToLoad, pageInfo);
                        }
                    }
                });
            } catch (EsRejectedExecutionException e) {
                try {
                    releaseIrrelevantSearchContexts(queryResults, docIdsToLoad, pageInfo);
                } finally {
                    callback.onFailure(e);
                }
            }
        }

        private void releaseIrrelevantSearchContexts(AtomicArray<QuerySearchResult> firstResults,
                                                     AtomicArray<IntArrayList> docIdsToLoad,
                                                     Optional<PageInfo> pageInfo) {
            // don't release searchcontexts yet, if we use scroll
            if (docIdsToLoad == null || pageInfo.isPresent()) {
                return;
            }

            for (AtomicArray.Entry<QuerySearchResult> entry : firstResults.asList()) {
                if (docIdsToLoad.get(entry.index) == null) {
                    DiscoveryNode node = nodes.get(entry.value.queryResult().shardTarget().nodeId());
                    if (node != null) {
                        searchServiceTransportAction.sendFreeContext(node, entry.value.queryResult().id(), EMPTY_SEARCH_REQUEST);
                    }
                }
            }
        }

        private void releaseAllContexts() {
            for (Map.Entry<SearchShardTarget, Long> entry : searchContextIds.entrySet()) {
                DiscoveryNode node = nodes.get(entry.getKey().nodeId());
                if (node != null) {
                    searchServiceTransportAction.sendFreeContext(node, entry.getValue(), EMPTY_SEARCH_REQUEST);
                }
            }
        }

        private void addShardFailure(int shardIndex, SearchShardTarget shardTarget, Throwable t) {
            if (TransportActions.isShardNotAvailableException(t)) {
                return;
            }
            if (shardFailures == null) {
                synchronized (shardFailuresMutex) {
                    if (shardFailures == null) {
                        shardFailures = new AtomicArray<>(requests.size());
                    }
                }
            }
            ShardSearchFailure failure = shardFailures.get(shardIndex);
            if (failure == null) {
                shardFailures.set(shardIndex, new ShardSearchFailure(t, shardTarget));
            } else {
                // the failure is already present, try and not override it with an exception that is less meaningless
                // for example, getting illegal shard state
                if (TransportActions.isReadOverrideException(t)) {
                    shardFailures.set(shardIndex, new ShardSearchFailure(t, shardTarget));
                }
            }
        }

        public int numShards() {
            return numShards;
        }

        @Override
        public void close() throws IOException {
            releaseAllContexts();
        }

        public List<Reference> outputs() {
            return outputs;
        }

        public QueryThenFetchNode searchNode() {
            return searchNode;
        }

        public ObjectArray<Object[]> toPage(SearchHit[] hits, List<FieldExtractor<SearchHit>> extractors) {
            ObjectArray<Object[]> rows = bigArrays.newObjectArray(hits.length);
            for (int r = 0; r < hits.length; r++) {
                rows.set(r, toRow(hits[r], extractors));
            }
            return rows;
        }

        private Object[] toRow(SearchHit hit, List<FieldExtractor<SearchHit>> extractors) {
            Object[] row = new Object[numColumns];
            for (int c = 0; c < numColumns; c++) {
                row[c] = extractors.get(c).extract(hit);
            }
            return row;
        }

        public Object[][] toRows(SearchHit[] hits, List<FieldExtractor<SearchHit>> extractors) {
            Object[][] rows = new Object[hits.length][numColumns];
            for (int r = 0; r < hits.length; r++) {
                rows[r] = toRow(hits[r], extractors);
            }
            return rows;
        }

        public void cleanAfterFirstPage() {
            for (int i = 0; i < docIdsToLoad.length(); i++) {
                docIdsToLoad.set(i, null);
            }
            // we still need the queryresults and searchContextIds
            for (int i = 0; i < fetchResults.length(); i++) {
                fetchResults.set(i, null);
            }
            if (shardFailures != null) {
                for (int i = 0; i < shardFailures.length(); i++) {
                    shardFailures.set(i, null);
                }
            }
            if (sortedShardList != null) {
                Arrays.fill(sortedShardList, null);
            }
            if (requests != null) {
                requests.clear();
            }
        }
    }

    public class QueryShardResponseListener implements ActionListener<QuerySearchResult> {

        private final int requestIdx;
        private final AtomicInteger totalOps;
        private final int expectedOps;
        private final QueryThenFetchContext ctx;
        private final FutureCallback<QueryThenFetchContext> callback;

        public QueryShardResponseListener(int requestIdx,
                                          AtomicInteger totalOps,
                                          QueryThenFetchContext ctx,
                                          FutureCallback<QueryThenFetchContext> callback) {

            this.requestIdx = requestIdx;
            this.ctx = ctx;
            this.totalOps = totalOps;
            this.callback = callback;
            this.expectedOps = ctx.queryResults.length();
        }

        @Override
        public void onResponse(QuerySearchResult querySearchResult) {
            Tuple<String, QueryShardRequest> requestTuple = ctx.requests.get(requestIdx);
            QueryShardRequest request = requestTuple.v2();

            querySearchResult.shardTarget(
                    new SearchShardTarget(requestTuple.v1(), request.index(), request.shardId()));
            ctx.searchContextIds.put(querySearchResult.shardTarget(), querySearchResult.id());
            ctx.queryResults.set(requestIdx, querySearchResult);
            if (totalOps.incrementAndGet() == expectedOps) {
                try {
                    moveToSecondPhase(ctx, callback);
                } catch (IOException e) {
                    raiseEarlyFailure(ctx, callback, e);
                }
            }
        }

        @Override
        public void onFailure(Throwable e) {
            raiseEarlyFailure(ctx, callback, e);
        }
    }
}
