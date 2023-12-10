/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.collect;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.assertThatThrownBy;
import static io.crate.testing.Asserts.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

import io.crate.common.collections.RefCountedItem;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.data.breaker.RamAccounting;
import io.crate.data.testing.TestingRowConsumer;
import io.crate.exceptions.JobKilledException;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.jobs.SharedShardContexts;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Routing;
import io.crate.metadata.RowGranularity;

public class CollectTaskTest extends ESTestCase {

    @Rule
    public MockitoRule initRule = MockitoJUnit.rule();

    private CollectTask collectTask;
    private RoutedCollectPhase collectPhase;
    private String localNodeId;
    private TestingRowConsumer consumer;
    private MapSideDataCollectOperation collectOperation;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        localNodeId = "dummyLocalNodeId";
        collectPhase = Mockito.mock(RoutedCollectPhase.class);
        Routing routing = Mockito.mock(Routing.class);
        when(routing.containsShards(localNodeId)).thenReturn(true);
        when(collectPhase.routing()).thenReturn(routing);
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.DOC);
        collectOperation = mock(MapSideDataCollectOperation.class);
        Mockito
            .doAnswer((Answer<Object>) invocation -> {
                Runnable runnable = invocation.getArgument(0);
                runnable.run();
                return null;
            })
            .when(collectOperation).launch(Mockito.any(), Mockito.anyString());
        consumer = new TestingRowConsumer();
        collectTask = new CollectTask(
            collectPhase,
            CoordinatorTxnCtx.systemTransactionContext(),
            collectOperation,
            RamAccounting.NO_ACCOUNTING,
            ramAccounting -> new OnHeapMemoryManager(ramAccounting::addBytes),
            consumer,
            mock(SharedShardContexts.class),
            Version.CURRENT,
            4096
        );
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testAddingSameContextTwice() throws Exception {
        RefCountedItem<IndexSearcher> mock1 = mock(RefCountedItem.class);
        RefCountedItem<IndexSearcher> mock2 = mock(RefCountedItem.class);
        try {
            collectTask.addSearcher(1, mock1);
            collectTask.addSearcher(1, mock2);

            fail("2nd addContext call should have raised an exception");
        } catch (IllegalArgumentException e) {
            verify(mock1, times(1)).close();
            verify(mock2, times(0)).close(); // would be closed via `kill` on the context
        }
    }

    @Test
    public void testThreadPoolNameForDocTables() throws Exception {
        String threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName).isEqualTo(ThreadPool.Names.SEARCH);
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
        assertThat(threadPoolExecutorName).isEqualTo(ThreadPool.Names.SEARCH);

        // partition values only of a partitioned doc table (single row collector)
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.PARTITION);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName).isEqualTo(ThreadPool.Names.SEARCH);

        // sys.nodes (single row collector)
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.NODE);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName).isEqualTo(ThreadPool.Names.GET);

        // sys.shards
        when(routing.containsShards(localNodeId)).thenReturn(true);
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.SHARD);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, true);
        assertThat(threadPoolExecutorName).isEqualTo(ThreadPool.Names.GET);
        when(routing.containsShards(localNodeId)).thenReturn(false);

        // information_schema.*
        when(collectPhase.maxRowGranularity()).thenReturn(RowGranularity.DOC);
        threadPoolExecutorName = CollectTask.threadPoolName(collectPhase, false);
        assertThat(threadPoolExecutorName).isEqualTo(ThreadPool.Names.SAME);
    }


    @SuppressWarnings("unchecked")
    @Test
    public void test_kill_before_start_triggers_consumer() throws Exception {
        RefCountedItem<IndexSearcher> searcher = mock(RefCountedItem.class);
        collectTask.addSearcher(1, searcher);
        collectTask.kill(JobKilledException.of(null));
        assertThatThrownBy(consumer::getBucket)
            .isExactlyInstanceOf(JobKilledException.class);
        verify(searcher, times(1)).close();
    }

    @Test
    public void test_kill_after_start_but_before_batch_iterator_created_future() throws Exception {
        var batchIterator = InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        CompletableFuture<BatchIterator<Row>> futureBatchIterator = new CompletableFuture<>();
        when(collectOperation.createIterator(Mockito.any(), Mockito.any(), anyBoolean(), Mockito.any()))
            .thenReturn(futureBatchIterator);
        collectTask.start();
        collectTask.kill(JobKilledException.of(null));
        futureBatchIterator.complete(batchIterator);
        assertThatThrownBy(consumer::getBucket)
            .isExactlyInstanceOf(JobKilledException.class);
    }

    @Test
    public void test_kill_after_start_in_different_thread_before_batch_iterator_created_future() throws Exception {
        var batchIterator = InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        CompletableFuture<BatchIterator<Row>> futureBatchIterator = new CompletableFuture<>();
        when(collectOperation.createIterator(Mockito.any(), Mockito.any(), anyBoolean(), Mockito.any()))
            .thenReturn(futureBatchIterator);
        collectTask.start();
        var killThreadStarted = new CountDownLatch(1);
        var threadsCompleted = new CountDownLatch(2);
        Thread t1 = new Thread(() -> {
            killThreadStarted.countDown();
            collectTask.kill(JobKilledException.of(null));
            threadsCompleted.countDown();
        });
        Thread t2 = new Thread(() -> {
            try {
                killThreadStarted.await(1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw Exceptions.toRuntimeException(e);
            }
            futureBatchIterator.complete(batchIterator);
            threadsCompleted.countDown();
        });
        t1.start();
        t2.start();
        try {
            consumer.getBucket();
        } catch (JobKilledException e) {
            // either success or failure is fine, depending on which thread is faster
        }
        threadsCompleted.await(1, TimeUnit.SECONDS);
    }

    @Test
    public void test_kill_after_start() throws Exception {
        var batchIterator = InMemoryBatchIterator.empty(SentinelRow.SENTINEL);
        when(collectOperation.createIterator(Mockito.any(), Mockito.any(), anyBoolean(), Mockito.any()))
            .thenReturn(CompletableFuture.completedFuture(batchIterator));
        collectTask.start();
        collectTask.kill(JobKilledException.of(null));

        // start() completes before kill gets a chance for the batchIterator to pick up the failure
        consumer.getBucket();
    }

    @Test
    public void test_add_searcher_after_kill_completed_does_close_searcher() throws Exception {
        RefCountedItem<IndexSearcher> searcher = mock(RefCountedItem.class);
        collectTask.kill(JobKilledException.of(null));
        assertThatThrownBy(() -> collectTask.addSearcher(0, searcher))
            .isExactlyInstanceOf(JobKilledException.class);
        verify(searcher, times(1)).close();
    }

    @Test
    public void test_consumer_finished_with_error_if_create_iterator_raises_an_error() throws Exception {
        when(collectOperation.createIterator(Mockito.any(), Mockito.any(), anyBoolean(), Mockito.any()))
            .thenThrow(new RuntimeException("create iterator failed"));
        collectTask.start();
        assertThatThrownBy(consumer::getBucket)
            .isExactlyInstanceOf(RuntimeException.class);
    }
}
