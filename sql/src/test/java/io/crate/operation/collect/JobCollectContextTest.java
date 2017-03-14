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

package io.crate.operation.collect;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.google.common.collect.ImmutableList;
import io.crate.action.job.SharedShardContexts;
import io.crate.breaker.RamAccountingContext;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.testing.TestingBatchConsumer;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class JobCollectContextTest extends RandomizedTest {

    private JobCollectContext jobCollectContext;
    private RoutedCollectPhase collectPhase;
    private String localNodeId;

    private RamAccountingContext ramAccountingContext = mock(RamAccountingContext.class);

    @Before
    public void setUp() throws Exception {
        localNodeId = "dummyLocalNodeId";
        collectPhase = Mockito.mock(RoutedCollectPhase.class);
        Routing routing = Mockito.mock(Routing.class);
        when(routing.containsShards(localNodeId)).thenReturn(true);
        when(collectPhase.routing()).thenReturn(routing);
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.DOC);
        jobCollectContext = new JobCollectContext(
            collectPhase,
            mock(MapSideDataCollectOperation.class),
            localNodeId,
            ramAccountingContext,
            new TestingBatchConsumer(),
            mock(SharedShardContexts.class));
    }

    @Test
    public void testAddingSameContextTwice() throws Exception {
        Engine.Searcher mock1 = mock(Engine.Searcher.class);
        Engine.Searcher mock2 = mock(Engine.Searcher.class);
        try {
            jobCollectContext.addSearcher(1, mock1);
            jobCollectContext.addSearcher(1, mock2);

            assertFalse(true); // second addContext call should have raised an exception
        } catch (IllegalArgumentException e) {
            verify(mock1, times(1)).close();
            verify(mock2, times(1)).close();
        }
    }

    @Test
    public void testCloseClosesSearchContexts() throws Exception {
        Engine.Searcher mock1 = mock(Engine.Searcher.class);
        Engine.Searcher mock2 = mock(Engine.Searcher.class);

        jobCollectContext.addSearcher(1, mock1);
        jobCollectContext.addSearcher(2, mock2);

        jobCollectContext.close();

        verify(mock1, times(1)).close();
        verify(mock2, times(1)).close();
        verify(ramAccountingContext, times(1)).close();
    }

    @Test
    public void testKillOnJobCollectContextPropagatesToCrateCollectors() throws Exception {
        Engine.Searcher mock1 = mock(Engine.Searcher.class);
        MapSideDataCollectOperation collectOperationMock = mock(MapSideDataCollectOperation.class);

        JobCollectContext jobCtx = new JobCollectContext(
            collectPhase,
            collectOperationMock,
            "localNodeId",
            ramAccountingContext,
            new TestingBatchConsumer(),
            mock(SharedShardContexts.class));

        jobCtx.addSearcher(1, mock1);
        CrateCollector collectorMock1 = mock(CrateCollector.class);
        CrateCollector collectorMock2 = mock(CrateCollector.class);

        when(collectOperationMock.createCollectors(eq(collectPhase), any(), eq(jobCtx)))
            .thenReturn(ImmutableList.of(collectorMock1, collectorMock2));
        jobCtx.prepare();
        jobCtx.start();
        jobCtx.kill(null);

        verify(collectorMock1, times(1)).kill(any(InterruptedException.class));
        verify(collectorMock2, times(1)).kill(any(InterruptedException.class));
        verify(mock1, times(1)).close();
        verify(ramAccountingContext, times(1)).close();
    }

    @Test
    public void testThreadPoolNameForDocTables() throws Exception {
        String threadPoolExecutorName = JobCollectContext.threadPoolName(collectPhase, localNodeId);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.SEARCH));
    }

    @Test
    public void testThreadPoolNameForNonDocTables() throws Exception {
        RoutedCollectPhase collectPhase = Mockito.mock(RoutedCollectPhase.class);
        Routing routing = Mockito.mock(Routing.class);
        when(collectPhase.routing()).thenReturn(routing);
        when(routing.containsShards(localNodeId)).thenReturn(false);

        // sys.cluster (single row collector)
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.CLUSTER);
        String threadPoolExecutorName = JobCollectContext.threadPoolName(collectPhase, localNodeId);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.PERCOLATE));

        // partition values only of a partitioned doc table (single row collector)
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.PARTITION);
        threadPoolExecutorName = JobCollectContext.threadPoolName(collectPhase, localNodeId);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.PERCOLATE));

        // sys.nodes (single row collector)
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.NODE);
        threadPoolExecutorName = JobCollectContext.threadPoolName(collectPhase, localNodeId);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.MANAGEMENT));

        // sys.shards
        when(routing.containsShards(localNodeId)).thenReturn(true);
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.SHARD);
        threadPoolExecutorName = JobCollectContext.threadPoolName(collectPhase, localNodeId);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.MANAGEMENT));
        when(routing.containsShards(localNodeId)).thenReturn(false);

        // information_schema.*
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.DOC);
        threadPoolExecutorName = JobCollectContext.threadPoolName(collectPhase, localNodeId);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.PERCOLATE));
    }
}
