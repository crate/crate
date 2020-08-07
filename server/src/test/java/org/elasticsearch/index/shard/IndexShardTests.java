/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.shard;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends IndexShardTestCase {

    @Test
    public void testRecordsForceMerges() throws IOException {
        IndexShard shard = newStartedShard(true);
        final String initialForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(initialForceMergeUUID, nullValue());
        final ForceMergeRequest firstForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(firstForceMergeRequest);
        final String secondForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(secondForceMergeUUID, notNullValue());
        assertThat(secondForceMergeUUID, equalTo(firstForceMergeRequest.forceMergeUUID()));
        final ForceMergeRequest secondForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(secondForceMergeRequest);
        final String thirdForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(thirdForceMergeUUID, notNullValue());
        assertThat(thirdForceMergeUUID, not(equalTo(secondForceMergeUUID)));
        assertThat(thirdForceMergeUUID, equalTo(secondForceMergeRequest.forceMergeUUID()));
        closeShards(shard);
    }

    @Test
    public void testClosesPreventsNewOperations() throws Exception {
        IndexShard indexShard = newStartedShard();
        closeShards(indexShard);
        assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
        expectThrows(IndexShardClosedException.class,
            () -> indexShard.acquirePrimaryOperationPermit(null, ThreadPool.Names.WRITE, ""));
        expectThrows(IndexShardClosedException.class,
            () -> indexShard.acquireAllPrimaryOperationsPermits(null, TimeValue.timeValueSeconds(30L)));
        expectThrows(IndexShardClosedException.class,
            () -> indexShard.acquireReplicaOperationPermit(indexShard.getPendingPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(), null, ThreadPool.Names.WRITE, ""));
        expectThrows(IndexShardClosedException.class,
            () -> indexShard.acquireAllReplicaOperationsPermits(indexShard.getPendingPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(), null, TimeValue.timeValueSeconds(30L)));
    }

    @Test
    public void testRunUnderPrimaryPermitRunsUnderPrimaryPermit() throws IOException {
        final IndexShard indexShard = newStartedShard(true);
        try {
            assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
            indexShard.runUnderPrimaryPermit(
                    () -> assertThat(indexShard.getActiveOperationsCount(), equalTo(1)),
                    e -> fail(e.toString()),
                    ThreadPool.Names.SAME,
                    "test");
                assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testRunUnderPrimaryPermitOnFailure() throws IOException {
        final IndexShard indexShard = newStartedShard(true);
        final AtomicBoolean invoked = new AtomicBoolean();
        try {
            indexShard.runUnderPrimaryPermit(
                    () -> {
                        throw new RuntimeException("failure");
                    },
                    e -> {
                        assertThat(e, instanceOf(RuntimeException.class));
                        assertThat(e.getMessage(), equalTo("failure"));
                        invoked.set(true);
                    },
                    ThreadPool.Names.SAME,
                    "test");
            assertTrue(invoked.get());
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testRunUnderPrimaryPermitDelaysToExecutorWhenBlocked() throws Exception {
        final IndexShard indexShard = newStartedShard(true);
        try {
            final PlainActionFuture<Releasable> onAcquired = new PlainActionFuture<>();
            indexShard.acquireAllPrimaryOperationsPermits(onAcquired, new TimeValue(Long.MAX_VALUE, TimeUnit.NANOSECONDS));
            final Releasable permit = onAcquired.actionGet();
            final CountDownLatch latch = new CountDownLatch(1);
            final String executorOnDelay =
                    randomFrom(ThreadPool.Names.FLUSH, ThreadPool.Names.GENERIC, ThreadPool.Names.MANAGEMENT, ThreadPool.Names.SAME);
            indexShard.runUnderPrimaryPermit(
                    () -> {
                        final String expectedThreadPoolName =
                                executorOnDelay.equals(ThreadPool.Names.SAME) ? "generic" : executorOnDelay.toLowerCase(Locale.ROOT);
                        assertThat(Thread.currentThread().getName(), Matchers.containsString(expectedThreadPoolName));
                        latch.countDown();
                    },
                    e -> fail(e.toString()),
                    executorOnDelay,
                    "test");
            permit.close();
            latch.await();
            // we could race and assert on the count before the permit is returned
            assertBusy(() -> assertThat(indexShard.getActiveOperationsCount(), equalTo(0)));
        } finally {
            closeShards(indexShard);
        }
    }


    @Test
    public void testAcquirePrimaryAllOperationsPermits() throws Exception {
        final IndexShard indexShard = newStartedShard(true);
        assertEquals(0, indexShard.getActiveOperationsCount());

        final CountDownLatch allPermitsAcquired = new CountDownLatch(1);

        final Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final List<PlainActionFuture<Releasable>> futures = new ArrayList<>(threads.length);
        final AtomicArray<Tuple<Boolean, Exception>> results = new AtomicArray<>(threads.length);
        final CountDownLatch allOperationsDone = new CountDownLatch(threads.length);

        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            final boolean singlePermit = randomBoolean();

            final PlainActionFuture<Releasable> future = new PlainActionFuture<Releasable>() {
                @Override
                public void onResponse(final Releasable releasable) {
                    if (singlePermit) {
                        assertThat(indexShard.getActiveOperationsCount(), greaterThan(0));
                    } else {
                        assertThat(indexShard.getActiveOperationsCount(), equalTo(0));
                    }
                    releasable.close();
                    super.onResponse(releasable);
                    results.setOnce(threadId, Tuple.tuple(Boolean.TRUE, null));
                    allOperationsDone.countDown();
                }

                @Override
                public void onFailure(final Exception e) {
                    results.setOnce(threadId, Tuple.tuple(Boolean.FALSE, e));
                    allOperationsDone.countDown();
                }
            };
            futures.add(threadId, future);

            threads[threadId] = new Thread(() -> {
                try {
                    allPermitsAcquired.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (singlePermit) {
                    indexShard.acquirePrimaryOperationPermit(future, ThreadPool.Names.WRITE, "");
                } else {
                    indexShard.acquireAllPrimaryOperationsPermits(future, TimeValue.timeValueHours(1L));
                }
                assertEquals(0, indexShard.getActiveOperationsCount());
            });
            threads[threadId].start();
        }

        final AtomicBoolean blocked = new AtomicBoolean();
        final CountDownLatch allPermitsTerminated = new CountDownLatch(1);

        final PlainActionFuture<Releasable> futureAllPermits = new PlainActionFuture<Releasable>() {
            @Override
            public void onResponse(final Releasable releasable) {
                try {
                    blocked.set(true);
                    allPermitsAcquired.countDown();
                    super.onResponse(releasable);
                    allPermitsTerminated.await();
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        indexShard.acquireAllPrimaryOperationsPermits(futureAllPermits, TimeValue.timeValueSeconds(30L));
        allPermitsAcquired.await();
        assertTrue(blocked.get());
        assertEquals(0, indexShard.getActiveOperationsCount());
        assertTrue("Expected no results, operations are blocked", results.asList().isEmpty());
        futures.forEach(future -> assertFalse(future.isDone()));

        allPermitsTerminated.countDown();

        final Releasable allPermits = futureAllPermits.get();
        assertTrue(futureAllPermits.isDone());

        assertTrue("Expected no results, operations are blocked", results.asList().isEmpty());
        futures.forEach(future -> assertFalse(future.isDone()));

        Releasables.close(allPermits);
        allOperationsDone.await();
        for (Thread thread : threads) {
            thread.join();
        }

        futures.forEach(future -> assertTrue(future.isDone()));
        assertEquals(threads.length, results.asList().size());
        results.asList().forEach(result -> {
            assertTrue(result.v1());
            assertNull(result.v2());
        });

        closeShards(indexShard);
    }

    @Test
    public void testShardStats() throws IOException {
        IndexShard shard = newStartedShard();
        ShardStats stats = new ShardStats(
            shard.routingEntry(),
            shard.shardPath(),
            new CommonStats(),
            shard.commitStats(),
            shard.seqNoStats(),
            shard.getRetentionLeaseStats()
        );
        assertThat(shard.shardPath().getRootDataPath().toString(), is(stats.getDataPath()));
        assertThat(shard.shardPath().getRootStatePath().toString(), is(stats.getStatePath()));
        assertThat(shard.shardPath().isCustomDataPath(), is(stats.isCustomDataPath()));
        assertThat(shard.getRetentionLeaseStats(), is(stats.getRetentionLeaseStats()));

        // try to serialize it to ensure values survive the serialization
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        stats = new ShardStats(in);

        assertThat(shard.shardPath().getRootDataPath().toString(), is(stats.getDataPath()));
        assertThat(shard.shardPath().getRootStatePath().toString(), is(stats.getStatePath()));
        assertThat(shard.shardPath().isCustomDataPath(), is(stats.isCustomDataPath()));
        assertThat(shard.getRetentionLeaseStats(), is(stats.getRetentionLeaseStats()));

        closeShards(shard);
    }
}
