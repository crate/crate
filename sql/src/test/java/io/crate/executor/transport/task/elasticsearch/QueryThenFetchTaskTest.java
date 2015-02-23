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

package io.crate.executor.transport.task.elasticsearch;

import com.carrotsearch.hppc.IntArrayList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.sql.query.CrateResultSorter;
import io.crate.action.sql.query.QueryShardRequest;
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.analyze.WhereClause;
import io.crate.executor.TaskResult;
import io.crate.metadata.Functions;
import io.crate.metadata.Routing;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.qtf.QueryThenFetchOperation;
import io.crate.planner.node.dql.QueryThenFetchNode;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.annotation.Nullable;
import java.util.*;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

public class QueryThenFetchTaskTest {

    private QueryThenFetchTask queryThenFetchTask;
    private TransportQueryShardAction transportQueryShardAction;
    private SearchServiceTransportAction searchServiceTransportAction;
    private CrateResultSorter crateResultSorter;
    private SearchPhaseController searchPhaseController = mock(SearchPhaseController.class);
    private DiscoveryNodes nodes = mock(DiscoveryNodes.class);
    private QueryThenFetchOperation queryThenFetchOperation;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Mock(answer = Answers.RETURNS_MOCKS)
    ClusterService clusterService;

    @Before
    public void prepare() {
        MockitoAnnotations.initMocks(this);
        QueryThenFetchNode searchNode = mock(QueryThenFetchNode.class);
        Map<String, Map<String, Set<Integer>>> locations = new TreeMap<>();
        Map<String, Set<Integer>> location1 = new TreeMap<>();
        location1.put("loc1", new HashSet<>(Arrays.asList(1)));
        locations.put("node_1", location1);
        Routing routing = new Routing(locations);
        when(searchNode.routing()).thenReturn(routing);

        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.blocks()).thenReturn(mock(ClusterBlocks.class));
        when(state.nodes()).thenReturn(nodes);

        transportQueryShardAction = mock(TransportQueryShardAction.class);
        searchServiceTransportAction = mock(SearchServiceTransportAction.class);
        crateResultSorter = mock(CrateResultSorter.class);

        ThreadPool testPool = new ThreadPool(getClass().getName());
        BigArrays mockedBigarrays = mock(BigArrays.class);
        queryThenFetchOperation = new QueryThenFetchOperation(
                clusterService,
                transportQueryShardAction,
                searchServiceTransportAction,
                searchPhaseController,
                testPool,
                mockedBigarrays,
                crateResultSorter
        );
        queryThenFetchTask = new QueryThenFetchTask(
                UUID.randomUUID(),
                queryThenFetchOperation,
                mock(Functions.class),
                searchNode
                );
    }

    @Test
    public void testAggregationInOutputs() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Operation not supported with symbol count()");
        new QueryThenFetchTask(
                UUID.randomUUID(),
                queryThenFetchOperation,
                mock(Functions.class),
                new QueryThenFetchNode(
                        new Routing(),
                        Arrays.<Symbol>asList(new Aggregation(CountAggregation.COUNT_STAR_FUNCTION, Collections.<Symbol>emptyList(),
                                Aggregation.Step.ITER, Aggregation.Step.FINAL)),
                        null,
                        null,
                        null,
                        null,
                        null,
                        WhereClause.MATCH_ALL,
                        null
                )
        );
    }

    @Test
    public void testFinishWithErrors() throws Throwable{
        ArgumentCaptor<QueryThenFetchOperation.QueryShardResponseListener> responseListener = ArgumentCaptor.forClass(
                QueryThenFetchOperation.QueryShardResponseListener.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                AtomicArray<IntArrayList> docIdsToLoad = (AtomicArray<IntArrayList>) invocation.getArguments()[0];
                docIdsToLoad.set(0, IntArrayList.from(1));
                return null;
            }
        }).when(searchPhaseController).fillDocIdsToLoad(Matchers.any(AtomicArray.class), any(ScoreDoc[].class));

        QuerySearchResult queryResult = mock(QuerySearchResult.class, Answers.RETURNS_DEEP_STUBS.get());

        SearchShardTarget target = mock(SearchShardTarget.class);
        when(target.nodeId()).thenReturn("node_1");
        when(queryResult.shardTarget()).thenReturn(target);

        queryThenFetchTask.start();
        verify(transportQueryShardAction).executeQuery(anyString(), any(QueryShardRequest.class), responseListener.capture());
        responseListener.getValue().onResponse(queryResult);

        ArgumentCaptor<SearchServiceListener> searchServiceListenerArgumentCaptor = ArgumentCaptor.forClass(SearchServiceListener.class);
        verify(searchServiceTransportAction).sendExecuteFetch(
                any(DiscoveryNode.class),
                any(ShardFetchSearchRequest.class), searchServiceListenerArgumentCaptor.capture());


        OutOfMemoryError oom = new OutOfMemoryError();
        searchServiceListenerArgumentCaptor.getValue().onFailure(oom);
        List<ListenableFuture<TaskResult>> result = queryThenFetchTask.result();

        Futures.addCallback(Futures.allAsList(result), new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(@Nullable List<TaskResult> result) {
                fail();
            }

            @Override
            public void onFailure(Throwable t) {
                assertThat(t.getMessage(), is("query failed on shard 0 ( OutOfMemoryError[null] )"));
            }
        });
    }

    @Test
    public void testErrorInQueryPhase() throws Throwable {
        ArgumentCaptor<QueryThenFetchOperation.QueryShardResponseListener> responseListener = ArgumentCaptor.forClass(
                QueryThenFetchOperation.QueryShardResponseListener.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                AtomicArray<IntArrayList> docIdsToLoad = (AtomicArray<IntArrayList>) invocation.getArguments()[0];
                docIdsToLoad.set(0, IntArrayList.from(1));
                return null;
            }
        }).when(searchPhaseController).fillDocIdsToLoad(any(AtomicArray.class), any(ScoreDoc[].class));

        queryThenFetchTask.start();
        verify(transportQueryShardAction).executeQuery(anyString(), any(QueryShardRequest.class), responseListener.capture());

        responseListener.getValue().onFailure(new OutOfMemoryError("no more memory"));
        List<ListenableFuture<TaskResult>> result = queryThenFetchTask.result();

        Futures.addCallback(Futures.allAsList(result), new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(@Nullable List<TaskResult> result) {
                fail();
            }

            @Override
            public void onFailure(Throwable t) {
                assertThat(t.getMessage(), is("no more memory"));
            }
        });

    }
}
