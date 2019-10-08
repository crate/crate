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

package io.crate.execution.engine.collect;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;
import io.crate.testing.TestingRowConsumer;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CollectTaskTest extends RandomizedTest {

    private CollectTask collectTask;
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
        collectTask = new CollectTask(
            collectPhase,
            CoordinatorTxnCtx.systemTransactionContext(),
            mock(MapSideDataCollectOperation.class),
            ramAccountingContext,
            new TestingRowConsumer(),
            mock(SharedShardContexts.class));
    }

    @Test
    public void testAddingSameContextTwice() throws Exception {
        Engine.Searcher mock1 = mock(Engine.Searcher.class);
        Engine.Searcher mock2 = mock(Engine.Searcher.class);
        try {
            collectTask.addSearcher(1, mock1);
            collectTask.addSearcher(1, mock2);

            assertFalse(true); // second addContext call should have raised an exception
        } catch (IllegalArgumentException e) {
            verify(mock1, times(1)).close();
            verify(mock2, times(1)).close();
        }
    }

    @Test
    public void testInnerCloseClosesSearchContexts() throws Exception {
        Engine.Searcher mock1 = mock(Engine.Searcher.class);
        Engine.Searcher mock2 = mock(Engine.Searcher.class);

        collectTask.addSearcher(1, mock1);
        collectTask.addSearcher(2, mock2);

        collectTask.innerClose();

        verify(mock1, times(1)).close();
        verify(mock2, times(1)).close();
        verify(ramAccountingContext, times(1)).close();
    }

    @Test
    public void testKillOnJobCollectContextPropagatesToCrateCollectors() throws Exception {
        Engine.Searcher mock1 = mock(Engine.Searcher.class);
        MapSideDataCollectOperation collectOperationMock = mock(MapSideDataCollectOperation.class);

        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        CollectTask jobCtx = new CollectTask(
            collectPhase,
            txnCtx,
            collectOperationMock,
            ramAccountingContext,
            new TestingRowConsumer(),
            mock(SharedShardContexts.class));

        jobCtx.addSearcher(1, mock1);

        BatchIterator<Row> batchIterator = mock(BatchIterator.class);
        when(collectOperationMock.createIterator(eq(txnCtx), eq(collectPhase), anyBoolean(), eq(jobCtx)))
            .thenReturn(batchIterator);
        jobCtx.prepare();
        jobCtx.start();
        jobCtx.kill(new JobKilledException());

        verify(batchIterator, times(1)).kill(any(JobKilledException.class));
        verify(mock1, times(1)).close();
        verify(ramAccountingContext, times(1)).close();
    }

    @Test
    public void testThreadPoolNameForDocTables() throws Exception {
        String threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
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
        String threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.SEARCH));

        // partition values only of a partitioned doc table (single row collector)
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.PARTITION);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.SEARCH));

        // sys.nodes (single row collector)
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.NODE);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.GET));

        // sys.shards
        when(routing.containsShards(localNodeId)).thenReturn(true);
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.SHARD);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.GET));
        when(routing.containsShards(localNodeId)).thenReturn(false);

        // information_schema.*
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.DOC);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, false);
        assertThat(threadPoolExecutorName, is(ThreadPool.Names.SAME));
    }
}
