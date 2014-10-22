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
import io.crate.action.sql.query.QueryShardRequest;
import io.crate.action.sql.query.TransportQueryShardAction;
import io.crate.executor.QueryResult;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Routing;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.planner.node.dql.QueryThenFetchNode;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.action.SearchServiceListener;
import org.elasticsearch.search.action.SearchServiceTransportAction;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
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

public class QueyThenFetchTaskTest {

    private QueryThenFetchTask queryThenFetchTask;
    private QueryThenFetchNode searchNode;
    private ClusterService clusterService;
    private TransportActionProvider transportActionProvider;
    private TransportQueryShardAction transportQueryShardAction;
    private SearchServiceTransportAction searchServiceTransportAction;
    private SearchPhaseController searchPhaseController;

    private DiscoveryNodes nodes = mock(DiscoveryNodes.class);

    @Before
    public void prepare() {
        searchNode = mock(QueryThenFetchNode.class);
        Map<String, Map<String, Set<Integer>>> locations = new HashMap<>();
        HashMap<String, Set<Integer>> location1 = new HashMap<String, Set<Integer>>();
        location1.put("loc1", new HashSet<Integer>(Arrays.asList(1)));
        locations.put("node_1", location1);
        Routing routing = new Routing(locations);
        when(searchNode.routing()).thenReturn(routing);

        clusterService = mock(ClusterService.class);
        ClusterState state = mock(ClusterState.class);
        when(clusterService.state()).thenReturn(state);
        when(state.blocks()).thenReturn(mock(ClusterBlocks.class));
        when(state.nodes()).thenReturn(nodes);


        transportActionProvider = mock(TransportActionProvider.class);
        transportQueryShardAction = mock(TransportQueryShardAction.class);
        searchServiceTransportAction = mock(SearchServiceTransportAction.class);

        when(transportActionProvider.searchServiceTransportAction())
                .thenReturn(searchServiceTransportAction);
        when(transportActionProvider.transportQueryShardAction())
                .thenReturn(transportQueryShardAction);
        searchPhaseController = mock(SearchPhaseController.class);
        queryThenFetchTask = new QueryThenFetchTask(searchNode,
                                                    clusterService,
                                                    ImmutableSettings.EMPTY,
                                                    transportActionProvider,
                                                    mock(ImplementationSymbolVisitor.class),
                                                    searchPhaseController,
                                                    new ThreadPool("testpool"));
    }


    @Test
    public void testFinishWithErrors() throws Throwable{
        ArgumentCaptor<QueryThenFetchTask.QueryShardResponseListener> responseListener = ArgumentCaptor.forClass(QueryThenFetchTask.QueryShardResponseListener.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                AtomicArray<IntArrayList> docIdsToLoad = (AtomicArray<IntArrayList>) invocation.getArguments()[0];
                docIdsToLoad.set(0, IntArrayList.from(1));
                return null;
            }
        }).when(searchPhaseController).fillDocIdsToLoad(any(AtomicArray.class), any(ScoreDoc[].class));

        QuerySearchResult queryResult = mock(QuerySearchResult.class, Answers.RETURNS_DEEP_STUBS.get());

        SearchShardTarget target = mock(SearchShardTarget.class);
        when(target.nodeId()).thenReturn("node_1");
        when(queryResult.shardTarget()).thenReturn(target);

        queryThenFetchTask.start();
        verify(transportQueryShardAction).execute(anyString(), any(QueryShardRequest.class), responseListener.capture());
        responseListener.getValue().onResponse(queryResult);

        ArgumentCaptor<SearchServiceListener> searchServiceListenerArgumentCaptor = ArgumentCaptor.forClass(SearchServiceListener.class);
        verify(searchServiceTransportAction).sendExecuteFetch(any(DiscoveryNode.class), any(FetchSearchRequest.class), searchServiceListenerArgumentCaptor.capture());


        OutOfMemoryError oom = new OutOfMemoryError();
        searchServiceListenerArgumentCaptor.getValue().onFailure(oom);
        List<ListenableFuture<QueryResult>> result = queryThenFetchTask.result();

        Futures.addCallback(Futures.allAsList(result), new FutureCallback<List<QueryResult>>() {
            @Override
            public void onSuccess(@Nullable List<QueryResult> result) {
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
        ArgumentCaptor<QueryThenFetchTask.QueryShardResponseListener> responseListener = ArgumentCaptor.forClass(QueryThenFetchTask.QueryShardResponseListener.class);
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                AtomicArray<IntArrayList> docIdsToLoad = (AtomicArray<IntArrayList>) invocation.getArguments()[0];
                docIdsToLoad.set(0, IntArrayList.from(1));
                return null;
            }
        }).when(searchPhaseController).fillDocIdsToLoad(any(AtomicArray.class), any(ScoreDoc[].class));

        queryThenFetchTask.start();
        verify(transportQueryShardAction).execute(anyString(), any(QueryShardRequest.class), responseListener.capture());

        responseListener.getValue().onFailure(new OutOfMemoryError("no more memory"));
        List<ListenableFuture<QueryResult>> result = queryThenFetchTask.result();

        Futures.addCallback(Futures.allAsList(result), new FutureCallback<List<QueryResult>>() {
            @Override
            public void onSuccess(@Nullable List<QueryResult> result) {
                fail();
            }

            @Override
            public void onFailure(Throwable t) {
                assertThat(t.getMessage(), is("no more memory"));
            }
        });

    }
}
