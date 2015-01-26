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

package io.crate.executor.transport.task.elasticsearch;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.query.CrateResultSorter;
import io.crate.action.sql.query.QueryShardRequest;
import io.crate.action.sql.query.QueryShardScrollRequest;
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.core.bigarray.MultiObjectArrayBigArray;
import io.crate.exceptions.Exceptions;
import io.crate.exceptions.FailedShardsException;
import io.crate.executor.*;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Routing;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
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
import org.elasticsearch.search.fetch.QueryFetchSearchResult;
import org.elasticsearch.search.fetch.ScrollQueryFetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryThenFetchTask extends JobTask implements PageableTask {

    private static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueMinutes(5L);
    private static final SymbolToFieldExtractor SYMBOL_TO_FIELD_EXTRACTOR =
            new SymbolToFieldExtractor<>(new SearchHitFieldExtractorFactory());

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    private Optional<TimeValue> keepAlive = Optional.absent();
    private int limit;
    private int offset;

    private final QueryThenFetchNode searchNode;
    private final TransportQueryShardAction transportQueryShardAction;
    private final SearchServiceTransportAction searchServiceTransportAction;
    private final SearchPhaseController searchPhaseController;
    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final CrateResultSorter crateResultSorter;

    private final SettableFuture<TaskResult> result;
    private final List<ListenableFuture<TaskResult>> results;

    private final Routing routing;
    private final AtomicArray<IntArrayList> docIdsToLoad;
    private final AtomicArray<QuerySearchResult> firstResults;
    private final AtomicArray<FetchSearchResult> fetchResults;
    private final DiscoveryNodes nodes;
    private final int numColumns;
    private final int numShards;
    private final ClusterState state;
    private final List<FieldExtractor<SearchHit>> extractors;
    volatile ScoreDoc[] sortedShardList;
    private volatile AtomicArray<ShardSearchFailure> shardFailures;
    private final Object shardFailuresMutex = new Object();
    private final Map<SearchShardTarget, Long> searchContextIds;
    private List<Tuple<String, QueryShardRequest>> requests;
    private List<Reference> references;

    /**
     * dummy request required to re-use the searchService transport
     */
    private final static SearchRequest EMPTY_SEARCH_REQUEST = new SearchRequest();


    public QueryThenFetchTask(UUID jobId,
                              Functions functions,
                              QueryThenFetchNode searchNode,
                              ClusterService clusterService,
                              TransportQueryShardAction transportQueryShardAction,
                              SearchServiceTransportAction searchServiceTransportAction,
                              SearchPhaseController searchPhaseController,
                              ThreadPool threadPool,
                              BigArrays bigArrays,
                              CrateResultSorter crateResultSorter) {
        super(jobId);
        this.searchNode = searchNode;
        this.transportQueryShardAction = transportQueryShardAction;
        this.searchServiceTransportAction = searchServiceTransportAction;
        this.searchPhaseController = searchPhaseController;
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.crateResultSorter = crateResultSorter;

        state = clusterService.state();
        nodes = state.nodes();

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<TaskResult>>asList(result);

        routing = searchNode.routing();
        this.limit = searchNode.limit();
        this.offset = searchNode.offset();

        SearchHitExtractorContext context = new SearchHitExtractorContext(functions, searchNode.outputs().size(), searchNode.partitionBy());
        extractors = new ArrayList<>(searchNode.outputs().size());
        for (Symbol symbol : searchNode.outputs()) {
            extractors.add(SYMBOL_TO_FIELD_EXTRACTOR.convert(symbol, context));
        }
        references = context.references();
        numShards = searchNode.routing().numShards();

        searchContextIds = new ConcurrentHashMap<>(numShards);
        docIdsToLoad = new AtomicArray<>(numShards);
        firstResults = new AtomicArray<>(numShards);
        fetchResults = new AtomicArray<>(numShards);
        numColumns = searchNode.outputs().size();
    }

    @Override
    public void start() {
        doStart(Optional.<PageInfo>absent());
    }

    @Override
    public void start(PageInfo pageInfo) {
        doStart(Optional.of(pageInfo));
    }

    private void doStart(Optional<PageInfo> pageInfo) {
        // create initial requests
        requests = prepareRequests(references, pageInfo);

        if (!routing.hasLocations() || requests.size() == 0) {
            result.set(
                    pageInfo.isPresent()
                            ? PageableTaskResult.EMPTY_PAGEABLE_RESULT
                            : TaskResult.EMPTY_RESULT
            );
        }

        state.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        AtomicInteger totalOps = new AtomicInteger(0);

        int requestIdx = -1;
        for (Tuple<String, QueryShardRequest> requestTuple : requests) {
            requestIdx++;
            state.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, requestTuple.v2().index());
            transportQueryShardAction.executeQuery(
                    requestTuple.v1(),
                    requestTuple.v2(),
                    new QueryShardResponseListener(requestIdx, firstResults, totalOps, pageInfo)
            );
        }
    }

    private List<Tuple<String, QueryShardRequest>> prepareRequests(List<Reference> outputs, Optional<PageInfo> pageInfo) {
        List<Tuple<String, QueryShardRequest>> requests = new ArrayList<>();
        Map<String, Map<String, Set<Integer>>> locations = searchNode.routing().locations();
        if (locations == null) {
            return requests;
        }

        int queryLimit;
        int queryOffset;

        if (pageInfo.isPresent()) {
            // fetch all, including all offset stuff
            queryLimit = this.offset + pageInfo.get().position() + pageInfo.get().size();
            queryOffset = 0;
        } else {
            queryLimit = this.limit;
            queryOffset = this.offset;
        }

        // only set keepAlive on pages Requests
        Optional<TimeValue> keepAliveValue = Optional.absent();
        if (pageInfo.isPresent()) {
            keepAliveValue = keepAlive.or(Optional.of(DEFAULT_KEEP_ALIVE));
        }

        for (Map.Entry<String, Map<String, Set<Integer>>> entry : locations.entrySet()) {
            String node = entry.getKey();
            for (Map.Entry<String, Set<Integer>> indexEntry : entry.getValue().entrySet()) {
                String index = indexEntry.getKey();
                Set<Integer> shards = indexEntry.getValue();

                for (Integer shard : shards) {
                    requests.add(new Tuple<>(
                            node,
                            new QueryShardRequest(
                                    index,
                                    shard,
                                    outputs,
                                    searchNode.orderBy(),
                                    searchNode.reverseFlags(),
                                    searchNode.nullsFirst(),
                                    queryLimit,
                                    queryOffset, // handle offset manually on handler for paged/scrolled calls
                                    searchNode.whereClause(),
                                    searchNode.partitionBy(),
                                    keepAliveValue
                            )
                    ));
                }
            }
        }
        return requests;
    }

    private void moveToSecondPhase(Optional<PageInfo> pageInfo) throws IOException {
        ScoreDoc[] lastEmittedDocs = null;
        if (pageInfo.isPresent()) {
            // first sort to determine the lastEmittedDocs
            // no offset and limit should be limit + offset
            int sortLimit = this.offset + pageInfo.get().size() + pageInfo.get().position();
            sortedShardList = crateResultSorter.sortDocs(firstResults, 0, sortLimit);
            lastEmittedDocs = searchPhaseController.getLastEmittedDocPerShard(
                    sortedShardList,
                    numShards);

            int fillOffset = pageInfo.get().position() + this.offset;

            // create a fetchrequest for all documents even those hit by the offset
            // to set the lastemitteddoc on the shard
            crateResultSorter.fillDocIdsToLoad(docIdsToLoad, sortedShardList, fillOffset);
        } else {
            sortedShardList = searchPhaseController.sortDocs(false, firstResults);
            searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);
        }

        //searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);
        if (docIdsToLoad.asList().isEmpty()) {
            finish(pageInfo);
            return;
        }

        final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());

        for (AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
            QuerySearchResult queryResult = firstResults.get(entry.index);
            DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
            ShardFetchSearchRequest fetchRequest = createFetchRequest(queryResult, entry, lastEmittedDocs);
            executeFetch(entry.index, queryResult.shardTarget(), counter, fetchRequest, node, pageInfo);
        }
    }

    private void executeFetch(final int shardIndex,
                              final SearchShardTarget shardTarget,
                              final AtomicInteger counter,
                              ShardFetchSearchRequest shardFetchSearchRequest,
                              DiscoveryNode node,
                              final Optional<PageInfo> pageInfo) {

        searchServiceTransportAction.sendExecuteFetch(
                node,
                shardFetchSearchRequest,
                new SearchServiceListener<FetchSearchResult>() {
                    @Override
                    public void onResult(FetchSearchResult result) {
                        result.shardTarget(shardTarget);
                        fetchResults.set(shardIndex, result);
                        if (counter.decrementAndGet() == 0) {
                            finish(pageInfo);
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        docIdsToLoad.set(shardIndex, null);
                        addShardFailure(shardIndex, shardTarget, t);
                        if (counter.decrementAndGet() == 0) {
                            finish(pageInfo);
                        }
                    }
                }
        );
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

    private ObjectArray<Object[]> toPage(SearchHit[] hits, List<FieldExtractor<SearchHit>> extractors) {
        ObjectArray<Object[]> rows = bigArrays.newObjectArray(hits.length);
        for (int r = 0; r < hits.length; r++) {
            Object[] row = new Object[numColumns];
            for (int c = 0; c < numColumns; c++) {
                row[c] = extractors.get(c).extract(hits[r]);
            }
            rows.set(r, row);
        }
        return rows;
    }

    private Object[][] toRows(SearchHit[] hits, List<FieldExtractor<SearchHit>> extractors) {
        Object[][] rows = new Object[hits.length][numColumns];

        for (int r = 0; r < hits.length; r++) {
            rows[r] = new Object[numColumns];
            for (int c = 0; c < numColumns; c++) {
                rows[r][c] = extractors.get(c).extract(hits[r]);
            }
        }
        return rows;
    }

    private void finish(final Optional<PageInfo> pageInfo) {
        try {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        if(shardFailures != null && shardFailures.length() > 0){
                            FailedShardsException ex = new FailedShardsException(shardFailures.toArray(
                                    new ShardSearchFailure[shardFailures.length()]));
                            result.setException(ex);
                            return;
                        }
                        InternalSearchResponse response = searchPhaseController.merge(sortedShardList, firstResults, fetchResults);

                        if (pageInfo.isPresent()) {
                            ObjectArray<Object[]> page = toPage(response.hits().hits(), extractors);
                            logger.trace("fetched {} rows for page {}", page.size(), pageInfo.get());
                            result.set(new QTFScrollTaskResult(page, 0, pageInfo.get()));
                        } else {
                            final Object[][] rows = toRows(response.hits().hits(), extractors);
                            result.set(new QueryResult(rows));
                        }
                    } catch (Throwable t) {
                        result.setException(t);
                    } finally {
                        releaseIrrelevantSearchContexts(firstResults, docIdsToLoad, pageInfo);
                    }
                }
            });
        } catch (EsRejectedExecutionException e) {
            try {
                releaseIrrelevantSearchContexts(firstResults, docIdsToLoad, pageInfo);
            } finally {
                result.setException(e);
            }
        }
    }

    class QTFScrollTaskResult implements PageableTaskResult {

        private final ObjectArray<Object[]> pageSource;
        private final Page page;
        private final long startIndexAtPageSource;
        private final PageInfo currentPageInfo;
        private final AtomicArray<QueryFetchSearchResult> queryFetchResults;

        public QTFScrollTaskResult(ObjectArray<Object[]> pageSource, long startIndexAtPageSource, PageInfo pageInfo) {
            this.pageSource = pageSource;
            this.startIndexAtPageSource = startIndexAtPageSource;
            this.currentPageInfo = pageInfo;
            this.queryFetchResults = new AtomicArray<>(numShards);
            this.page = new BigArrayPage(pageSource, startIndexAtPageSource, currentPageInfo.size());
        }

        private void fetchFromSource(final long needToFetch, final FutureCallback<ObjectArray<Object[]>> callback) {
            final AtomicInteger numOps = new AtomicInteger(numShards);

            final Scroll scroll = new Scroll(keepAlive.or(DEFAULT_KEEP_ALIVE));

            for (final Map.Entry<SearchShardTarget, Long> entry : searchContextIds.entrySet()) {
                DiscoveryNode node = nodes.get(entry.getKey().nodeId());

                QueryShardScrollRequest request = new QueryShardScrollRequest(entry.getValue(), scroll, (int)needToFetch);

                transportQueryShardAction.executeScroll(node.id(), request, new ActionListener<ScrollQueryFetchSearchResult>() {
                    @Override
                    public void onResponse(ScrollQueryFetchSearchResult scrollQueryFetchSearchResult) {
                        QueryFetchSearchResult qfsResult = scrollQueryFetchSearchResult.result();
                        qfsResult.shardTarget(entry.getKey());

                        int opNum = numOps.decrementAndGet();
                        queryFetchResults.set(opNum, qfsResult);
                        if (opNum == 0) {
                            try {
                                threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                                    @Override
                                    public void run() {
                                        try {
                                            ScoreDoc[] sortedShardList = searchPhaseController.sortDocs(true, queryFetchResults);
                                            InternalSearchResponse response = searchPhaseController.merge(sortedShardList, queryFetchResults, queryFetchResults);
                                            final SearchHit[] hits = response.hits().hits();
                                            logger.trace("fetched {} hits from QTF. needed {}", hits.length, needToFetch);
                                            final ObjectArray<Object[]> page = toPage(hits, extractors);
                                            callback.onSuccess(page);
                                        } catch (Throwable e) {
                                            onFailure(e);
                                        }
                                    }
                                });
                            } catch (EsRejectedExecutionException e) {
                                logger.error("error merging searchResults of QTFScrollTaskResult", e);
                                onFailure(e);
                            }
                        }
                        logger.trace("op #{}. received scroll response from shard {}.", numOps.get(), entry.getKey());
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        callback.onFailure(t);
                        try {
                            close();
                        } catch (IOException e) {
                            logger.error("error closing QTFScrollTaskResult", e);
                        }
                    }
                });
            }
        }

        @Override
        public ListenableFuture<PageableTaskResult> fetch(final PageInfo pageInfo) {
            Preconditions.checkArgument(
                    pageInfo.position() == (this.currentPageInfo.size() + this.currentPageInfo.position()),
                    "QueryThenFetchTask can only page forward without gaps");

            final long restSize = pageSource.size() - startIndexAtPageSource - currentPageInfo.size();
            final SettableFuture<PageableTaskResult> future = SettableFuture.create();
            if (restSize >= pageInfo.size()) {
                logger.trace("we can take all results for page {} from results of last page", pageInfo);
                // don't need to fetch nuttin'
                future.set(
                        new QTFScrollTaskResult(
                                pageSource,
                                startIndexAtPageSource + currentPageInfo.size(),
                                pageInfo)
                );
            } else if (restSize < 0) {
                logger.trace("last page ({}) got less results than requested, we are at the end. no result for page {}.", currentPageInfo, pageInfo);
                // if restSize is less than 0, we got less than requested
                // and in that can safely assume that we're exhausted
                future.set(PageableTaskResult.EMPTY_PAGEABLE_RESULT);
                try {
                    close();
                } catch (IOException e) {
                    logger.error("error closing QTFScrollTaskResult", e);
                }
            } else if (restSize == 0) {
                logger.trace("need to fetch {} for page {}.", pageInfo.size(), pageInfo);
                fetchFromSource(pageInfo.size(), new FutureCallback<ObjectArray<Object[]>>() {
                    @Override
                    public void onSuccess(@Nullable ObjectArray<Object[]> result) {
                        if (result.size() == 0) {
                            future.set(PageableTaskResult.EMPTY_PAGEABLE_RESULT);
                            try {
                                close();
                            } catch (IOException e) {
                                logger.error("error closing QTFScrollTaskResult", e);
                            }
                        } else {
                            future.set(new QTFScrollTaskResult(result, 0, pageInfo));
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        future.setException(t);
                    }
                });
            } else if (restSize > 0) {
                logger.trace("need to fetch {} for page {}. still got {} from last page", pageInfo.size() - restSize, pageInfo, restSize);
                // we got a rest, need to combine stuff
                fetchFromSource(pageInfo.size() - restSize, new FutureCallback<ObjectArray<Object[]>>() {
                    @Override
                    public void onSuccess(@Nullable ObjectArray<Object[]> result) {

                        MultiObjectArrayBigArray<Object[]> merged = new MultiObjectArrayBigArray<>(
                                pageSource.size() - restSize,
                                restSize + result.size(),
                                pageSource,
                                result
                        );
                        future.set(new QTFScrollTaskResult(merged, 0, pageInfo));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        future.setException(t);
                    }
                });
            }
            return future;
        }

        @Override
        public Page page() {
            return page;
        }

        @Override
        public Object[][] rows() {
            throw new UnsupportedOperationException("QTFScrollTaskResult does not support rows()");
        }

        @javax.annotation.Nullable
        @Override
        public String errorMessage() {
            return null;
        }

        @Override
        public void close() throws IOException {
            releaseAllContexts();
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

    protected ShardFetchSearchRequest createFetchRequest(QuerySearchResult queryResult,
                                                    AtomicArray.Entry<IntArrayList> entry, @Nullable ScoreDoc[] lastEmittedDocsPerShard) {
        if (lastEmittedDocsPerShard != null) {
            ScoreDoc lastEmittedDoc = lastEmittedDocsPerShard[entry.index];
            return new ShardFetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value, lastEmittedDoc);
        }
        return new ShardFetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value);
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("Can't have upstreamResults");
    }

    /**
     * set the keep alive value for the search context on the shards
     */
    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = Optional.of(keepAlive);
    }

    class QueryShardResponseListener implements ActionListener<QuerySearchResult> {

        private final int requestIdx;
        private final AtomicArray<QuerySearchResult> firstResults;
        private final AtomicInteger totalOps;
        private final int expectedOps;
        private final Optional<PageInfo> pageInfo;

        public QueryShardResponseListener(int requestIdx,
                                          AtomicArray<QuerySearchResult> firstResults,
                                          AtomicInteger totalOps, Optional<PageInfo> pageInfo) {

            this.requestIdx = requestIdx;
            this.firstResults = firstResults;
            this.totalOps = totalOps;
            this.pageInfo = pageInfo;
            this.expectedOps = firstResults.length();
        }

        @Override
        public void onResponse(QuerySearchResult querySearchResult) {
            Tuple<String, QueryShardRequest> requestTuple = requests.get(requestIdx);
            QueryShardRequest request = requestTuple.v2();


            querySearchResult.shardTarget(
                    new SearchShardTarget(requestTuple.v1(), request.index(), request.shardId()));
            searchContextIds.put(querySearchResult.shardTarget(), querySearchResult.id());
            firstResults.set(requestIdx, querySearchResult);
            if (totalOps.incrementAndGet() == expectedOps) {
                try {
                    moveToSecondPhase(pageInfo);
                } catch (IOException e) {
                    raiseEarlyFailure(e);
                }
            }
        }

        @Override
        public void onFailure(Throwable e) {
            raiseEarlyFailure(e);
        }
    }

    private void raiseEarlyFailure(Throwable t) {
        for (AtomicArray.Entry<QuerySearchResult> entry : firstResults.asList()) {
            try {
                DiscoveryNode node = nodes.get(entry.value.shardTarget().nodeId());
                if (node != null) {
                    searchServiceTransportAction.sendFreeContext(node, entry.value.id(), EMPTY_SEARCH_REQUEST);
                }
            } catch (Throwable t1) {
                logger.trace("failed to release context", t1);
            }
        }
        t = Exceptions.unwrap(t);
        if (t instanceof QueryPhaseExecutionException) {
            result.setException(t.getCause());
            return;
        }
        result.setException(t);
    }

    static class SearchHitExtractorContext extends SymbolToFieldExtractor.Context {
        private final List<ReferenceInfo> partitionBy;

        public SearchHitExtractorContext(Functions functions, int size, List<ReferenceInfo> partitionBy) {
            super(functions, size);
            this.partitionBy = partitionBy;
        }
    }

    static class SearchHitFieldExtractorFactory implements FieldExtractorFactory<SearchHit, SearchHitExtractorContext> {

        @Override
        public FieldExtractor<SearchHit> build(Reference field, SearchHitExtractorContext context) {
            final ColumnIdent columnIdent = field.info().ident().columnIdent();
            if (columnIdent.isSystemColumn()) {
                if (DocSysColumns.VERSION.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getVersion();
                        }
                    };
                } else if (DocSysColumns.ID.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return new BytesRef(hit.getId());
                        }
                    };
                } else if (DocSysColumns.DOC.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getSource();
                        }
                    };
                } else if (DocSysColumns.RAW.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getSourceRef().toBytesRef();
                        }
                    };
                } else if (DocSysColumns.SCORE.equals(columnIdent)) {
                    return new ESFieldExtractor() {
                        @Override
                        public Object extract(SearchHit hit) {
                            return hit.getScore();
                        }
                    };
                } else {
                    throw new UnsupportedOperationException(
                            String.format(Locale.ENGLISH, "Unsupported system column %s", columnIdent.name()));
                }
            } else if (context.partitionBy.contains(field.info())) {
                return new ESFieldExtractor.PartitionedByColumnExtractor(field, context.partitionBy);
            } else {
                return new ESFieldExtractor.Source(columnIdent);
            }
        }
    }

}
