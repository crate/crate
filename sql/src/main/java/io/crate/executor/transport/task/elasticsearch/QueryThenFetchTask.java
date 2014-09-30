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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.query.QueryShardRequest;
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.exceptions.Exceptions;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.metadata.ColumnIdent;
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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QueryPhaseExecutionException;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class QueryThenFetchTask extends Task<QueryResult> {

    private final ESLogger logger = Loggers.getLogger(this.getClass());

    private final QueryThenFetchNode searchNode;
    private final TransportQueryShardAction transportQueryShardAction;
    private final SearchServiceTransportAction searchServiceTransportAction;
    private final SearchPhaseController searchPhaseController;
    private final ThreadPool threadPool;
    private final SettableFuture<QueryResult> result;
    private final List<ListenableFuture<QueryResult>> results;

    private final Routing routing;
    private final AtomicArray<IntArrayList> docIdsToLoad;
    private final List<Tuple<String, QueryShardRequest>> requests;
    private final AtomicArray<QuerySearchResult> firstResults;
    private final AtomicArray<FetchSearchResult> fetchResults;
    private final DiscoveryNodes nodes;
    private final ESFieldExtractor[] extractor;
    private final int numColumns;
    private final ClusterState state;
    volatile ScoreDoc[] sortedShardList;
    private volatile AtomicArray<ShardSearchFailure> shardFailures;
    private final Object shardFailuresMutex = new Object();


    /**
     * dummy request required to re-use the searchService transport
     */
    private final static SearchRequest EMPTY_SEARCH_REQUEST = new SearchRequest();


    public QueryThenFetchTask(UUID jobId,
                              QueryThenFetchNode searchNode,
                              ClusterService clusterService,
                              TransportQueryShardAction transportQueryShardAction,
                              SearchServiceTransportAction searchServiceTransportAction,
                              SearchPhaseController searchPhaseController,
                              ThreadPool threadPool) {
        super(jobId);
        this.searchNode = searchNode;
        this.transportQueryShardAction = transportQueryShardAction;
        this.searchServiceTransportAction = searchServiceTransportAction;
        this.searchPhaseController = searchPhaseController;
        this.threadPool = threadPool;

        state = clusterService.state();
        nodes = state.nodes();

        result = SettableFuture.create();
        results = Arrays.<ListenableFuture<QueryResult>>asList(result);

        routing = searchNode.routing();
        requests = prepareRequests(routing.locations());
        docIdsToLoad = new AtomicArray<>(requests.size());
        firstResults = new AtomicArray<>(requests.size());
        fetchResults = new AtomicArray<>(requests.size());

        extractor = buildExtractor(searchNode.outputs());
        numColumns = searchNode.outputs().size();
    }

    private ESFieldExtractor[] buildExtractor(final List<Symbol> outputs) {
        ESFieldExtractor[] extractors = new ESFieldExtractor[outputs.size()];
        int i = 0;
        for (Symbol output : outputs) {
            assert output instanceof Reference;
            Reference reference = ((Reference) output);
            final ColumnIdent columnIdent = reference.info().ident().columnIdent();
            if (DocSysColumns.VERSION.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getVersion();
                    }
                };
            } else if (DocSysColumns.ID.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return new BytesRef(hit.getId());
                    }
                };
            } else if (DocSysColumns.DOC.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getSource();
                    }
                };
            } else if (DocSysColumns.RAW.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getSourceRef().toBytesRef();
                    }
                };
            } else if (DocSysColumns.SCORE.equals(columnIdent)) {
                extractors[i] = new ESFieldExtractor() {
                    @Override
                    public Object extract(SearchHit hit) {
                        return hit.getScore();
                    }
                };
            } else if (searchNode.partitionBy().contains(reference.info())) {
                extractors[i] = new ESFieldExtractor.PartitionedByColumnExtractor(
                        reference, searchNode.partitionBy()
                );
            } else {
                extractors[i] = new ESFieldExtractor.Source(columnIdent);
            }
            i++;
        }
        return extractors;
    }

    @Override
    public void start() {
        if (!routing.hasLocations() || requests.size() == 0) {
            result.set(QueryResult.EMPTY_RESULT);
        }

        state.blocks().globalBlockedRaiseException(ClusterBlockLevel.READ);
        AtomicInteger totalOps = new AtomicInteger(0);

        int requestIdx = -1;
        for (Tuple<String, QueryShardRequest> requestTuple : requests) {
            requestIdx++;
            state.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, requestTuple.v2().index());
            transportQueryShardAction.execute(
                    requestTuple.v1(),
                    requestTuple.v2(),
                    new QueryShardResponseListener(requestIdx, firstResults, totalOps)
            );
        }
    }

    private List<Tuple<String, QueryShardRequest>> prepareRequests(Map<String, Map<String, Set<Integer>>> locations) {
        List<Tuple<String, QueryShardRequest>> requests = new ArrayList<>();
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
                                    searchNode.outputs(),
                                    searchNode.orderBy(),
                                    searchNode.reverseFlags(),
                                    searchNode.nullsFirst(),
                                    searchNode.limit(),
                                    searchNode.offset(),
                                    searchNode.whereClause(),
                                    searchNode.partitionBy()
                            )
                    ));
                }
            }
        }
        return requests;
    }

    private void moveToSecondPhase() throws IOException {
        // boolean useScroll = !useSlowScroll && request.scroll() != null;
        sortedShardList = searchPhaseController.sortDocs(false, firstResults);
        searchPhaseController.fillDocIdsToLoad(docIdsToLoad, sortedShardList);

        if (docIdsToLoad.asList().isEmpty()) {
            finish();
            return;
        }

        final ScoreDoc[] lastEmittedDocPerShard = null;
        // searchPhaseController.getLastEmittedDocPerShard(request, sortedShardList, firstResults.length());

        final AtomicInteger counter = new AtomicInteger(docIdsToLoad.asList().size());
        for (AtomicArray.Entry<IntArrayList> entry : docIdsToLoad.asList()) {
            QuerySearchResult queryResult = firstResults.get(entry.index);
            DiscoveryNode node = nodes.get(queryResult.shardTarget().nodeId());
            FetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult, entry, lastEmittedDocPerShard);
            executeFetch(entry.index, queryResult.shardTarget(), counter, fetchSearchRequest, node);
        }
    }

    private void executeFetch(final int shardIndex,
                              final SearchShardTarget shardTarget,
                              final AtomicInteger counter,
                              FetchSearchRequest fetchSearchRequest,
                              DiscoveryNode node) {

        searchServiceTransportAction.sendExecuteFetch(
                node,
                fetchSearchRequest,
                new SearchServiceListener<FetchSearchResult>() {
                    @Override
                    public void onResult(FetchSearchResult result) {
                        result.shardTarget(shardTarget);
                        fetchResults.set(shardIndex, result);
                        if (counter.decrementAndGet() == 0) {
                            finish();
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        docIdsToLoad.set(shardIndex, null);
                        addShardFailure(shardIndex, shardTarget, t);
                        if (counter.decrementAndGet() == 0) {
                            finish();
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

    private void finish() {
        try {
            threadPool.executor(ThreadPool.Names.SEARCH).execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        InternalSearchResponse response = searchPhaseController.merge(sortedShardList, firstResults, fetchResults);
                        final SearchHit[] hits = response.hits().hits();
                        final Object[][] rows = new Object[hits.length][numColumns];

                        for (int r = 0; r < hits.length; r++) {
                            rows[r] = new Object[numColumns];
                            for (int c = 0; c < numColumns; c++) {
                                rows[r][c] = extractor[c].extract(hits[r]);
                            }
                        }
                        result.set(new QueryResult(rows));
                    } catch (Throwable t) {
                        result.setException(t);
                    } finally {
                        releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
                    }
                }
            });
        } catch (EsRejectedExecutionException e) {
            try {
                releaseIrrelevantSearchContexts(firstResults, docIdsToLoad);
            } finally {
                result.setException(e);
            }
        }
    }

    private void releaseIrrelevantSearchContexts(AtomicArray<QuerySearchResult> firstResults,
                                                 AtomicArray<IntArrayList> docIdsToLoad) {
        if (docIdsToLoad == null) {
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

    protected FetchSearchRequest createFetchRequest(QuerySearchResult queryResult,
                                                    AtomicArray.Entry<IntArrayList> entry,
                                                    ScoreDoc[] lastEmittedDocPerShard) {
        if (lastEmittedDocPerShard != null) {
            ScoreDoc lastEmittedDoc = lastEmittedDocPerShard[entry.index];
            return new FetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value, lastEmittedDoc);
        } else {
            return new FetchSearchRequest(EMPTY_SEARCH_REQUEST, queryResult.id(), entry.value);
        }
    }
    @Override
    public List<ListenableFuture<QueryResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("Can't have upstreamResults");
    }

    class QueryShardResponseListener implements ActionListener<QuerySearchResult> {

        private final int requestIdx;
        private final AtomicArray<QuerySearchResult> firstResults;
        private final AtomicInteger totalOps;
        private final int expectedOps;

        public QueryShardResponseListener(int requestIdx,
                                          AtomicArray<QuerySearchResult> firstResults,
                                          AtomicInteger totalOps) {

            this.requestIdx = requestIdx;
            this.firstResults = firstResults;
            this.totalOps = totalOps;
            this.expectedOps = firstResults.length();
        }

        @Override
        public void onResponse(QuerySearchResult querySearchResult) {
            Tuple<String, QueryShardRequest> requestTuple = requests.get(requestIdx);
            QueryShardRequest request = requestTuple.v2();

            querySearchResult.shardTarget(
                    new SearchShardTarget(requestTuple.v1(), request.index(), request.shardId()));
            firstResults.set(requestIdx, querySearchResult);
            if (totalOps.incrementAndGet() == expectedOps) {
                try {
                    moveToSecondPhase();
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
}
