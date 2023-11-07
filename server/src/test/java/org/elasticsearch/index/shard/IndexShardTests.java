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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.common.lucene.Lucene.cleanLuceneIndex;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.elasticsearch.index.translog.Translog.UNSET_AUTO_GENERATED_TIMESTAMP;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.BytesRef;
import org.assertj.core.data.Offset;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.CommitStats;
import org.elasticsearch.index.engine.DocIdSeqNoAndSource;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SourceFieldMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.store.StoreUtils;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.index.translog.TranslogTests;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.store.MockFSDirectoryFactory;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Assert;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.RandomizedTest;

import io.crate.Constants;
import io.crate.action.FutureActionListener;
import io.crate.common.collections.Tuple;
import io.crate.common.exceptions.Exceptions;
import io.crate.common.unit.TimeValue;
import io.crate.metadata.doc.DocSysColumns;

/**
 * Simple unit-test IndexShard related operations.
 */
public class IndexShardTests extends IndexShardTestCase {

    @Test
    public void testRecordsForceMerges() throws Exception {
        IndexShard shard = newStartedShard(true);
        final String initialForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(initialForceMergeUUID).isNull();
        final ForceMergeRequest firstForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(firstForceMergeRequest);
        final String secondForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(secondForceMergeUUID).isNotNull();
        assertThat(secondForceMergeUUID).isEqualTo(firstForceMergeRequest.forceMergeUUID());
        final ForceMergeRequest secondForceMergeRequest = new ForceMergeRequest().maxNumSegments(1);
        shard.forceMerge(secondForceMergeRequest);
        final String thirdForceMergeUUID = ((InternalEngine) shard.getEngine()).getForceMergeUUID();
        assertThat(thirdForceMergeUUID).isNotNull();
        assertThat(thirdForceMergeUUID).isNotEqualTo(secondForceMergeUUID);
        assertThat(thirdForceMergeUUID).isEqualTo(secondForceMergeRequest.forceMergeUUID());
        closeShards(shard);
    }

    @Test
    public void testWriteShardState() throws Exception {
        try (NodeEnvironment env = newNodeEnvironment()) {
            ShardId id = new ShardId("foo", "fooUUID", 1);
            boolean primary = randomBoolean();
            AllocationId allocationId = randomBoolean() ? null : randomAllocationId();
            ShardStateMetadata state1 = new ShardStateMetadata(primary, "fooUUID", allocationId);
            ShardStateMetadata.FORMAT.writeAndCleanup(state1, env.availableShardPaths(id));
            ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT
                .loadLatestState(logger, NamedXContentRegistry.EMPTY, env.availableShardPaths(id));
            assertThat(shardStateMetadata).isEqualTo(state1);

            ShardStateMetadata state2 = new ShardStateMetadata(primary, "fooUUID", allocationId);
            ShardStateMetadata.FORMAT.writeAndCleanup(state2, env.availableShardPaths(id));
            shardStateMetadata = ShardStateMetadata.FORMAT
                .loadLatestState(logger, NamedXContentRegistry.EMPTY, env.availableShardPaths(id));
            assertThat(shardStateMetadata).isEqualTo(state1);

            ShardStateMetadata state3 = new ShardStateMetadata(primary, "fooUUID", allocationId);
            ShardStateMetadata.FORMAT.writeAndCleanup(state3, env.availableShardPaths(id));
            shardStateMetadata = ShardStateMetadata.FORMAT
                .loadLatestState(logger, NamedXContentRegistry.EMPTY, env.availableShardPaths(id));
            assertThat(shardStateMetadata).isEqualTo(state3);
            assertThat("fooUUID").isEqualTo(state3.indexUUID);
        }
    }

    @Test
    public void testPersistenceStateMetadataPersistence() throws Exception {
        IndexShard shard = newStartedShard();
        Path shardStatePath = shard.shardPath().getShardStatePath();
        ShardStateMetadata shardStateMetadata  = ShardStateMetadata.FORMAT
            .loadLatestState(logger, NamedXContentRegistry.EMPTY, shardStatePath);
        assertThat(getShardStateMetadata(shard)).isEqualTo(shardStateMetadata);
        ShardRouting routing = shard.shardRouting;
        IndexShardTestCase.updateRoutingEntry(shard, routing);

        shardStateMetadata  = ShardStateMetadata.FORMAT
            .loadLatestState(logger, NamedXContentRegistry.EMPTY, shardStatePath);
        assertThat(shardStateMetadata).isEqualTo(getShardStateMetadata(shard));
        assertThat(shardStateMetadata).isEqualTo(
            new ShardStateMetadata(
                routing.primary(),
                shard.indexSettings().getUUID(),
                routing.allocationId())
        );

        routing = TestShardRouting.relocate(shard.shardRouting, "some node", 42L);
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        shardStateMetadata  = ShardStateMetadata.FORMAT
            .loadLatestState(logger, NamedXContentRegistry.EMPTY, shardStatePath);
        assertThat(shardStateMetadata).isEqualTo(getShardStateMetadata(shard));
        assertThat(shardStateMetadata).isEqualTo(
            new ShardStateMetadata(
                routing.primary(),
                shard.indexSettings().getUUID(),
                routing.allocationId())
        );
        closeShards(shard);
    }

    @Test
    public void testFailShard() throws Exception {
        allowShardFailures();
        IndexShard shard = newStartedShard();
        ShardPath shardPath = shard.shardPath();
        assertThat(shardPath).isNotNull();
        // fail shard
        shard.failShard("test shard fail", new CorruptIndexException("", ""));
        shard.close("do not assert history", false);
        shard.store().close();
        // check state file still exists
        ShardStateMetadata shardStateMetadata  = ShardStateMetadata.FORMAT
            .loadLatestState(logger, NamedXContentRegistry.EMPTY, shardPath.getShardStatePath());

        assertThat(shardStateMetadata).isEqualTo(getShardStateMetadata(shard));
        // but index can't be opened for a failed shard
        assertThat(StoreUtils.canOpenIndex(
            logger,
            shardPath.resolveIndex(),
            shard.shardId(),
            (shardId, lockTimeoutMS, details) -> new DummyShardLock(shardId)
        ))
            .as("store index should be corrupted")
            .isFalse();
    }

    @Test
    public void testShardStateMetaHashCodeEquals() {
        AllocationId allocationId = randomBoolean() ? null : randomAllocationId();
        ShardStateMetadata meta = new ShardStateMetadata(
            randomBoolean(),
            randomRealisticUnicodeOfCodepointLengthBetween(1, 10),
            allocationId);

        assertThat(new ShardStateMetadata(meta.primary, meta.indexUUID, meta.allocationId)).isEqualTo(meta);
        assertThat(
            new ShardStateMetadata(meta.primary, meta.indexUUID, meta.allocationId).hashCode())
            .isEqualTo(meta.hashCode());

        assertThat(new ShardStateMetadata(!meta.primary, meta.indexUUID, meta.allocationId)).isNotEqualTo(meta);
        assertThat(new ShardStateMetadata(!meta.primary, meta.indexUUID + "foo", meta.allocationId)).isNotEqualTo(meta);
        assertThat(new ShardStateMetadata(!meta.primary, meta.indexUUID + "foo", randomAllocationId())).isNotEqualTo(meta);
        Set<Integer> hashCodes = new HashSet<>();
        for (int i = 0; i < 30; i++) { // just a sanity check that we impl hashcode
            allocationId = randomBoolean() ? null : randomAllocationId();
            meta = new ShardStateMetadata(randomBoolean(),
                                          randomRealisticUnicodeOfCodepointLengthBetween(1, 10), allocationId);
            hashCodes.add(meta.hashCode());
        }
        assertThat(hashCodes)
            .as("more than one unique hashcode expected but got: " + hashCodes.size())
            .hasSizeGreaterThan(1);
    }

    @Test
    public void testClosesPreventsNewOperations() throws Exception {
        IndexShard indexShard = newStartedShard();
        closeShards(indexShard);
        assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
        assertThatThrownBy(() -> indexShard.acquirePrimaryOperationPermit(null, ThreadPool.Names.WRITE, ""))
            .isExactlyInstanceOf(IndexShardClosedException.class);
        assertThatThrownBy(() -> indexShard.acquireAllPrimaryOperationsPermits(null, TimeValue.timeValueSeconds(30L)))
            .isExactlyInstanceOf(IndexShardClosedException.class);
        assertThatThrownBy(() -> indexShard.acquireReplicaOperationPermit(indexShard.getPendingPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(), null, ThreadPool.Names.WRITE, "")
        ).isExactlyInstanceOf(IndexShardClosedException.class);
        assertThatThrownBy(() -> indexShard.acquireAllReplicaOperationsPermits(indexShard.getPendingPrimaryTerm(), SequenceNumbers.UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(), null, TimeValue.timeValueSeconds(30L))
        ).isExactlyInstanceOf(IndexShardClosedException.class);
    }

    @Test
    public void testRunUnderPrimaryPermitRunsUnderPrimaryPermit() throws Exception {
        final IndexShard indexShard = newStartedShard(true);
        try {
            assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
            indexShard.runUnderPrimaryPermit(
                    () -> assertThat(indexShard.getActiveOperationsCount()).isEqualTo(1),
                    e -> fail(e.toString()),
                    ThreadPool.Names.SAME,
                    "test");
                assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testRunUnderPrimaryPermitOnFailure() throws Exception {
        final IndexShard indexShard = newStartedShard(true);
        final AtomicBoolean invoked = new AtomicBoolean();
        try {
            indexShard.runUnderPrimaryPermit(
                    () -> {
                        throw new RuntimeException("failure");
                    },
                    e -> {
                        assertThat(e).isExactlyInstanceOf(RuntimeException.class);
                        assertThat(e.getMessage()).isEqualTo("failure");
                        invoked.set(true);
                    },
                    ThreadPool.Names.SAME,
                    "test");
            assertThat(invoked.get()).isTrue();
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
            final Releasable permit = FutureUtils.get(onAcquired);
            final CountDownLatch latch = new CountDownLatch(1);
            final String executorOnDelay =
                    randomFrom(ThreadPool.Names.FLUSH, ThreadPool.Names.GENERIC, ThreadPool.Names.MANAGEMENT, ThreadPool.Names.SAME);
            indexShard.runUnderPrimaryPermit(
                    () -> {
                        final String expectedThreadPoolName =
                                executorOnDelay.equals(ThreadPool.Names.SAME) ? "generic" : executorOnDelay.toLowerCase(Locale.ROOT);
                        assertThat(Thread.currentThread().getName()).contains(expectedThreadPoolName);
                        latch.countDown();
                    },
                    e -> fail(e.toString()),
                    executorOnDelay,
                    "test");
            permit.close();
            latch.await();
            // we could race and assert on the count before the permit is returned
            assertBusy(() -> assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0));
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testAcquirePrimaryAllOperationsPermits() throws Exception {
        final IndexShard indexShard = newStartedShard(true);
        assertThat(0).isEqualTo(indexShard.getActiveOperationsCount());

        final CountDownLatch allPermitsAcquired = new CountDownLatch(1);

        final Thread[] threads = new Thread[randomIntBetween(2, 5)];
        final List<PlainActionFuture<Releasable>> futures = new ArrayList<>(threads.length);
        final AtomicArray<Tuple<Boolean, Exception>> results = new AtomicArray<>(threads.length);
        final CountDownLatch allOperationsDone = new CountDownLatch(threads.length);

        for (int i = 0; i < threads.length; i++) {
            final int threadId = i;
            final boolean singlePermit = randomBoolean();

            final PlainActionFuture<Releasable> future = new PlainActionFuture<>() {
                @Override
                public void onResponse(final Releasable releasable) {
                    if (singlePermit) {
                        assertThat(indexShard.getActiveOperationsCount()).isGreaterThan(0);
                    } else {
                        assertThat(indexShard.getActiveOperationsCount()).isEqualTo(IndexShard.OPERATIONS_BLOCKED);
                    }
                    releasable.close();
                    super.onResponse(releasable);
                    results.setOnce(threadId, new Tuple<>(Boolean.TRUE, null));
                    allOperationsDone.countDown();
                }

                @Override
                public void onFailure(final Exception e) {
                    results.setOnce(threadId, new Tuple<>(Boolean.FALSE, e));
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
            });
            threads[threadId].start();
        }

        final AtomicBoolean blocked = new AtomicBoolean();
        final CountDownLatch allPermitsTerminated = new CountDownLatch(1);

        final PlainActionFuture<Releasable> futureAllPermits = new PlainActionFuture<>() {
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
        assertThat(blocked.get()).isTrue();
        assertThat(IndexShard.OPERATIONS_BLOCKED).isEqualTo(indexShard.getActiveOperationsCount());
        assertThat(results.asList())
            .as("Expected no results, operations are blocked")
            .isEmpty();
        futures.forEach(future -> assertThat(future.isDone()).isFalse());

        allPermitsTerminated.countDown();

        final Releasable allPermits = futureAllPermits.get();
        assertThat(futureAllPermits.isDone()).isTrue();

        assertThat(results.asList())
            .as("Expected no results, operations are blocked")
            .isEmpty();
        futures.forEach(future -> assertThat(future.isDone()).isFalse());

        Releasables.close(allPermits);
        allOperationsDone.await();
        for (Thread thread : threads) {
            thread.join();
        }

        futures.forEach(future -> assertThat(future.isDone()).isTrue());
        assertThat(threads.length).isEqualTo(results.asList().size());
        results.asList().forEach(result -> {
            assertThat(result.v1()).isTrue();
            assertThat(result.v2()).isNull();
        });

        closeShards(indexShard);
    }

    @Test
    public void testShardStats() throws Exception {
        IndexShard shard = newStartedShard();
        ShardStats stats = new ShardStats(
            shard.routingEntry(),
            shard.shardPath(),
            new CommonStats(),
            shard.commitStats(),
            shard.seqNoStats(),
            shard.getRetentionLeaseStats()
        );
        assertThat(shard.shardPath().getRootDataPath().toString()).isEqualTo(stats.getDataPath());
        assertThat(shard.shardPath().getRootStatePath().toString()).isEqualTo(stats.getStatePath());
        assertThat(shard.shardPath().isCustomDataPath()).isEqualTo(stats.isCustomDataPath());
        assertThat(shard.getRetentionLeaseStats()).isEqualTo(stats.getRetentionLeaseStats());

        // try to serialize it to ensure values survive the serialization
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        stats = new ShardStats(in);

        assertThat(shard.shardPath().getRootDataPath().toString()).isEqualTo(stats.getDataPath());
        assertThat(shard.shardPath().getRootStatePath().toString()).isEqualTo(stats.getStatePath());
        assertThat(shard.shardPath().isCustomDataPath()).isEqualTo(stats.isCustomDataPath());
        assertThat(shard.getRetentionLeaseStats()).isEqualTo(stats.getRetentionLeaseStats());

        closeShards(shard);
    }

    @Test
    public void testIndexingOperationListenersIsInvokedOnRecovery() throws Exception {
        IndexShard shard = newStartedShard(true);
        updateMappings(shard, IndexMetadata.builder(shard.indexSettings.getIndexMetadata())
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}").build());
        indexDoc(shard, "0", "{\"foo\" : \"bar\"}");
        deleteDoc(shard, "0");
        indexDoc(shard, "1", "{\"foo\" : \"bar\"}");
        shard.refresh("test");

        AtomicInteger preIndex = new AtomicInteger();
        AtomicInteger postIndex = new AtomicInteger();
        AtomicInteger preDelete = new AtomicInteger();
        AtomicInteger postDelete = new AtomicInteger();
        IndexingOperationListener listener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                postIndex.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                postDelete.incrementAndGet();

            }
        };
        final IndexShard newShard = reinitShard(shard, listener);
        recoverShardFromStore(newShard);
        assertThat(preIndex.get()).isEqualTo(2);
        assertThat(postIndex.get()).isEqualTo(2);
        assertThat(preDelete.get()).isEqualTo(1);
        assertThat(postDelete.get()).isEqualTo(1);

        closeShards(newShard);
    }

    @Test
    public void testTranslogRecoverySyncsTranslog() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoverShardFromStore(primary);

        indexDoc(primary, "0", "{\"foo\" : \"bar\"}");
        IndexShard replica = newShard(primary.shardId(), false, "n2", metaData, null);
        recoverReplica(replica, primary, (shard, discoveryNode) ->
            new RecoveryTarget(shard, discoveryNode, recoveryListener) {
                @Override
                public void indexTranslogOperations(
                    List<Translog.Operation> operations,
                    int totalTranslogOps,
                    long maxSeenAutoIdTimestamp,
                    long maxSeqNoOfUpdatesOrDeletes,
                    RetentionLeases retentionLeases,
                    long mappingVersion,
                    ActionListener<Long> listener) {
                    super.indexTranslogOperations(
                        operations,
                        totalTranslogOps,
                        maxSeenAutoIdTimestamp,
                        maxSeqNoOfUpdatesOrDeletes,
                        retentionLeases,
                        mappingVersion,
                        ActionListener.wrap(
                            r -> {
                                assertThat(replica.isSyncNeeded()).isEqualTo(false);
                                listener.onResponse(r);
                            },
                            listener::onFailure
                        ));
                }
            }, true, true);

        closeShards(primary, replica);
    }

    @Test
    public void testRecoverFromTranslog() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, randomLongBetween(1, Long.MAX_VALUE)).build();
        IndexShard primary = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        List<Translog.Operation> operations = new ArrayList<>();
        int numTotalEntries = randomIntBetween(0, 10);
        int numCorruptEntries = 0;
        for (int i = 0; i < numTotalEntries; i++) {
            if (randomBoolean()) {
                operations.add(
                    new Translog.Index(
                        "1",
                        0,
                        primary.getPendingPrimaryTerm(),
                        1,
                        "{\"foo\" : \"bar\"}".getBytes(StandardCharsets.UTF_8), -1));
            } else {
                // corrupt entry
                operations.add(
                    new Translog.Index(
                        "2",
                        1,
                        primary.getPendingPrimaryTerm(),
                        1,
                        "{\"foo\" : \"bar}".getBytes(StandardCharsets.UTF_8), -1));
                numCorruptEntries++;
            }
        }
        Translog.Snapshot snapshot = TestTranslog.newSnapshotFromOperations(operations);
        primary.markAsRecovering(
            "store",
            new RecoveryState(
                primary.routingEntry(),
                getFakeDiscoNode(primary.routingEntry().currentNodeId()),
                null));
        recoverFromStore(primary);

        primary.recoveryState().getTranslog().totalOperations(snapshot.totalOperations());
        primary.recoveryState().getTranslog().totalOperationsOnStart(snapshot.totalOperations());
        primary.state = IndexShardState.RECOVERING;
        // translog recovery on the next line would otherwise fail as we are in POST_RECOVERY
        primary.runTranslogRecovery(
            primary.getEngine(),
            snapshot,
            Engine.Operation.Origin.LOCAL_TRANSLOG_RECOVERY,
            primary.recoveryState().getTranslog()::incrementRecoveredOperations);
        assertThat(primary.recoveryState().getTranslog().recoveredOperations())
            .isEqualTo(numTotalEntries - numCorruptEntries);
        closeShards(primary);
    }

    @Test
    public void testShardActiveDuringInternalRecovery() throws Exception {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "0");
        shard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode(
            "foo",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT
        );
        shard.markAsRecovering("for testing", new RecoveryState(shard.routingEntry(), localNode, null));
        // Shard is still inactive since we haven't started recovering yet
        assertThat(shard.isActive()).isFalse();
        shard.prepareForIndexRecovery();
        // Shard is still inactive since we haven't started recovering yet
        assertThat(shard.isActive()).isFalse();
        shard.openEngineAndRecoverFromTranslog();
        // Shard should now be active since we did recover:
        assertThat(shard.isActive()).isTrue();
        closeShards(shard);
    }

    @Test
    public void testShardActiveDuringPeerRecovery() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoverShardFromStore(primary);

        indexDoc(primary, "0", "{\"foo\" : \"bar\"}");
        IndexShard replica = newShard(primary.shardId(), false, "n2", metaData, null);
        DiscoveryNode localNode = new DiscoveryNode(
            "foo",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        // Shard is still inactive since we haven't started recovering yet
        assertThat(replica.isActive()).isEqualTo(false);
        recoverReplica(replica, primary, (shard, discoveryNode) ->
            new RecoveryTarget(shard, discoveryNode, recoveryListener) {
                @Override
                public void indexTranslogOperations(
                    final List<Translog.Operation> operations,
                    final int totalTranslogOps,
                    final long maxAutoIdTimestamp,
                    final long maxSeqNoOfUpdatesOrDeletes,
                    final RetentionLeases retentionLeases,
                    final long mappingVersion,
                    final ActionListener<Long> listener) {
                    super.indexTranslogOperations(
                        operations,
                        totalTranslogOps,
                        maxAutoIdTimestamp,
                        maxSeqNoOfUpdatesOrDeletes,
                        retentionLeases,
                        mappingVersion,
                        ActionListener.wrap(
                            checkpoint -> {
                                listener.onResponse(checkpoint);
                                // Shard should now be active since we did recover:
                                assertThat(replica.isActive()).isTrue();
                            },
                            listener::onFailure));
                }
            }, false, true);

        closeShards(primary, replica);
    }

    @Test
    public void testRefreshListenersDuringPeerRecovery() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoverShardFromStore(primary);

        indexDoc(primary, "0", "{\"foo\" : \"bar\"}");
        Consumer<IndexShard> assertListenerCalled = shard -> {
            AtomicBoolean called = new AtomicBoolean();
            shard.addRefreshListener(null, b -> {
                assertThat(b).isEqualTo(false);
                called.set(true);
            });
            assertThat(called.get()).isTrue();
        };
        IndexShard replica = newShard(primary.shardId(), false, "n2", metaData, null);
        DiscoveryNode localNode = new DiscoveryNode(
            "foo",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT
        );
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        assertListenerCalled.accept(replica);
        recoverReplica(replica, primary, (shard, discoveryNode) ->
            new RecoveryTarget(shard, discoveryNode, recoveryListener) {
                // we're only checking that listeners are called when the engine is open, before there is no point
                @Override
                public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
                    super.prepareForTranslogOperations(
                        totalTranslogOps,
                        ActionListener.wrap(
                            r -> {
                                assertListenerCalled.accept(replica);
                                listener.onResponse(r);
                            }, listener::onFailure));
                }

                @Override
                public void indexTranslogOperations(
                    final List<Translog.Operation> operations,
                    final int totalTranslogOps,
                    final long maxAutoIdTimestamp,
                    final long maxSeqNoOfUpdatesOrDeletes,
                    final RetentionLeases retentionLeases,
                    final long mappingVersion,
                    final ActionListener<Long> listener) {
                    super.indexTranslogOperations(
                        operations,
                        totalTranslogOps,
                        maxAutoIdTimestamp,
                        maxSeqNoOfUpdatesOrDeletes,
                        retentionLeases,
                        mappingVersion,
                        ActionListener.wrap(
                            r -> {
                                assertListenerCalled.accept(replica);
                                listener.onResponse(r);
                            }, listener::onFailure));
                }

                @Override
                public void finalizeRecovery(long globalCheckpoint,
                                             long trimAboveSeqNo,
                                             ActionListener<Void> listener) {
                    super.finalizeRecovery(
                        globalCheckpoint,
                        trimAboveSeqNo,
                        ActionListener.wrap(
                            r -> {
                                assertListenerCalled.accept(replica);
                                listener.onResponse(r);
                            }, listener::onFailure));
                }
            }, false, true);

        closeShards(primary, replica);
    }

    @Test
    public void testRecoverFromLocalShard() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("source")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();

        IndexShard sourceShard = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoverShardFromStore(sourceShard);

        indexDoc(sourceShard, "0", "{\"foo\" : \"bar\"}");
        indexDoc(sourceShard, "1", "{\"foo\" : \"bar\"}");
        sourceShard.refresh("test");


        ShardRouting targetRouting = newShardRouting(
            new ShardId("index_1", "index_1", 0),
            "n1",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.LocalShardsRecoverySource.INSTANCE);

        IndexShard targetShard;
        DiscoveryNode localNode = new DiscoveryNode(
            "foo",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT);
        Map<String, MappingMetadata> requestedMappingUpdates = ConcurrentCollections.newConcurrentMap();
        targetShard = newShard(targetRouting);
        targetShard.markAsRecovering("store", new RecoveryState(targetShard.routingEntry(), localNode, null));

        Consumer<MappingMetadata> mappingConsumer =  mapping ->
            assertThat(requestedMappingUpdates.put(Constants.DEFAULT_MAPPING_TYPE, mapping)).isNull();

        final IndexShard differentIndex = newShard(new ShardId("index_2", "index_2", 0), true);
        recoverShardFromStore(differentIndex);
        assertThatThrownBy(() -> {
            final FutureActionListener<Boolean> future = new FutureActionListener<>();
            targetShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(sourceShard, differentIndex), future);
            future.get();
        }).isExactlyInstanceOf(IllegalArgumentException.class);
        closeShards(differentIndex);

        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        targetShard.recoverFromLocalShards(mappingConsumer, Arrays.asList(sourceShard), future);
        assertThat(future.get()).isTrue();
        RecoveryState recoveryState = targetShard.recoveryState();
        assertThat(RecoveryState.Stage.DONE).isEqualTo(recoveryState.getStage());
        assertThat(recoveryState.getIndex().fileDetails().size() > 0).isTrue();
        for (var entry : recoveryState.getIndex().fileDetails().values()) {
            if (entry.reused()) {
                assertThat(entry.recovered()).isEqualTo(0);
            } else {
                assertThat(entry.recovered()).isEqualTo(entry.length());
            }
        }
        // check that local checkpoint of new primary is properly tracked after recovery
        assertThat(targetShard.getLocalCheckpoint()).isEqualTo(1L);
        assertThat(targetShard.getReplicationTracker().getGlobalCheckpoint()).isEqualTo(1L);
        IndexShardTestCase.updateRoutingEntry(
            targetShard,
            ShardRoutingHelper.moveToStarted(targetShard.routingEntry()));
        assertThat(targetShard.getReplicationTracker().getTrackedLocalCheckpointForShard(
            targetShard.routingEntry().allocationId().getId()).getLocalCheckpoint()).isEqualTo(1L);
        assertDocCount(targetShard, 2);
        // now check that it's persistent ie. that the added shards are committed
        final IndexShard newShard = reinitShard(targetShard);
        recoverShardFromStore(newShard);
        assertDocCount(newShard, 2);
        closeShards(newShard);

        assertThat(
            requestedMappingUpdates.get("default").source().string())
            .isEqualTo("{\"properties\":{\"foo\":{\"type\":\"text\",\"position\":1}}}");
        closeShards(sourceShard, targetShard);
    }

    @Test
    public void testDocStats() throws Exception {
        IndexShard indexShard = null;
        try {
            indexShard = newStartedShard(
                false,
                Settings.builder()
                    .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), 0)
                    .build()
            );
            long numDocs = randomIntBetween(2, 32); // at least two documents so we have docs to delete
            long numDocsToDelete = randomLongBetween(1, numDocs);
            for (int i = 0; i < numDocs; i++) {
                final String id = Integer.toString(i);
                indexDoc(indexShard, id);
            }
            if (randomBoolean()) {
                indexShard.refresh("test");
            } else {
                indexShard.flush(new FlushRequest());
            }
            {
                IndexShard shard = indexShard;
                assertBusy(() -> {
                    ThreadPool threadPool = shard.getThreadPool();
                    assertThat(threadPool.relativeTimeInMillis()).isGreaterThan(shard.getLastSearcherAccess());
                });
                long prevAccessTime = shard.getLastSearcherAccess();
                DocsStats docsStats = indexShard.docStats();
                assertThat(shard.getLastSearcherAccess())
                    .as("searcher was marked as accessed")
                    .isEqualTo(prevAccessTime);
                assertThat(docsStats.getCount()).isEqualTo(numDocs);
                try (Engine.Searcher searcher = indexShard.acquireSearcher("test")) {
                    assertThat(searcher.getIndexReader().numDocs() <= docsStats.getCount()).isTrue();
                }
                assertThat(docsStats.getDeleted()).isEqualTo(0L);
                assertThat(docsStats.getAverageSizeInBytes()).isGreaterThan(0L);
            }

            List<Integer> ids = randomSubsetOf(
                Math.toIntExact(numDocsToDelete),
                IntStream.range(0, Math.toIntExact(numDocs)).boxed().collect(Collectors.toList()));
            for (Integer i : ids) {
                String id = Integer.toString(i);
                deleteDoc(indexShard, id);
                indexDoc(indexShard, id);
            }
            // Need to update and sync the global checkpoint and the retention
            // leases for the soft-deletes retention MergePolicy.
            if (indexShard.indexSettings.isSoftDeleteEnabled()) {
                final long newGlobalCheckpoint = indexShard.getLocalCheckpoint();
                if (indexShard.routingEntry().primary()) {
                    indexShard.updateLocalCheckpointForShard(
                        indexShard.routingEntry().allocationId().getId(),
                        indexShard.getLocalCheckpoint());
                    indexShard.updateGlobalCheckpointForShard(
                        indexShard.routingEntry().allocationId().getId(),
                        indexShard.getLocalCheckpoint());
                    indexShard.syncRetentionLeases();
                } else {
                    indexShard.updateGlobalCheckpointOnReplica(newGlobalCheckpoint, "test");

                    RetentionLeases retentionLeases = indexShard.getRetentionLeases();
                    indexShard.updateRetentionLeasesOnReplica(new RetentionLeases(
                        retentionLeases.primaryTerm(), retentionLeases.version() + 1,
                        retentionLeases.leases().stream()
                            .map(lease -> new RetentionLease(
                                lease.id(),
                                newGlobalCheckpoint + 1,
                                lease.timestamp(),
                                ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE)
                            ).collect(Collectors.toList())));
                }
                indexShard.sync();
            }
            // flush the buffered deletes
            FlushRequest flushRequest = new FlushRequest();
            flushRequest.force(false);
            flushRequest.waitIfOngoing(false);
            indexShard.flush(flushRequest);

            if (randomBoolean()) {
                indexShard.refresh("test");
            }
            {
                DocsStats docStats = indexShard.docStats();
                try (Engine.Searcher searcher = indexShard.acquireSearcher("test")) {
                    assertThat(searcher.getIndexReader().numDocs() <= docStats.getCount()).isTrue();
                }
                assertThat(docStats.getCount()).isEqualTo(numDocs);
            }

            // merge them away
            ForceMergeRequest forceMergeRequest = new ForceMergeRequest();
            forceMergeRequest.maxNumSegments(1);
            indexShard.forceMerge(forceMergeRequest);

            if (randomBoolean()) {
                indexShard.refresh("test");
            } else {
                indexShard.flush(new FlushRequest());
            }
            {
                DocsStats docStats = indexShard.docStats();
                assertThat(docStats.getCount()).isEqualTo(numDocs);
                assertThat(docStats.getDeleted()).isEqualTo(0L);
                assertThat(docStats.getAverageSizeInBytes()).isGreaterThan(0L);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testEstimateTotalDocSize() throws Exception {
        IndexShard indexShard = null;
        try {
            indexShard = newStartedShard(true);
            updateMappings(
                indexShard,
                IndexMetadata
                    .builder(indexShard.indexSettings.getIndexMetadata())
                    .putMapping(
                        "{ \"properties\": {" +
                            "\"count\": { \"type\": \"integer\", \"position\": 1}, " +
                            "\"point\": { \"type\": \"float\", \"position\": 2}, " +
                            "\"description\": { \"type\": \"text\", \"position\": 3}}}"
                    ).build());

            int numDoc = randomIntBetween(100, 200);
            for (int i = 0; i < numDoc; i++) {
                String doc = Strings.toString(
                    JsonXContent.builder()
                        .startObject()
                        .field("count", randomInt())
                        .field("point", randomFloat())
                        .field("description", RandomizedTest.randomUnicodeOfCodepointLength(100))
                        .endObject()
                );
                indexDoc(indexShard, Integer.toString(i), doc);
            }

            assertThat(indexShard.docStats().getTotalSizeInBytes())
                .as("Without flushing, segment sizes should be zero")
                .isEqualTo(0L);

            if (randomBoolean()) {
                indexShard.flush(new FlushRequest());
            } else {
                indexShard.refresh("test");
            }
            {
                DocsStats docsStats = indexShard.docStats();
                StoreStats storeStats = indexShard.storeStats();
                assertThat(storeStats.sizeInBytes())
                           .isGreaterThan(numDoc * 100L); // A doc should be more than 100 bytes.

                assertThat(docsStats.getTotalSizeInBytes())
                    .as("Estimated total document size is too small compared with the stored size")
                    .isGreaterThanOrEqualTo(storeStats.sizeInBytes() * 80 / 100);
                assertThat( docsStats.getTotalSizeInBytes())
                    .as("Estimated total document size is too large compared with the stored size")
                    .isLessThanOrEqualTo(storeStats.sizeInBytes() * 120 / 100);
            }
            // Do some updates and deletes, then recheck the correlation again.
            updateMappings(
                indexShard,
                IndexMetadata.builder(
                    indexShard.indexSettings.getIndexMetadata()
                ).putMapping(
                    "{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}"
                ).build());
            for (int i = 0; i < numDoc / 2; i++) {
                if (randomBoolean()) {
                    deleteDoc(indexShard, Integer.toString(i));
                } else {
                    indexDoc(indexShard, Integer.toString(i), "{\"foo\": \"bar\"}");
                }
            }
            if (randomBoolean()) {
                indexShard.flush(new FlushRequest());
            } else {
                indexShard.refresh("test");
            }
            {
                DocsStats docsStats = indexShard.docStats();
                StoreStats storeStats = indexShard.storeStats();
                assertThat(docsStats.getTotalSizeInBytes())
                    .as("Estimated total document size is too small compared with the stored size")
                    .isGreaterThanOrEqualTo(storeStats.sizeInBytes() * 80 / 100);
                assertThat(docsStats.getTotalSizeInBytes())
                    .as("Estimated total document size is too large compared with the stored size")
                    .isLessThanOrEqualTo(storeStats.sizeInBytes() * 120 / 100);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    /**
     * here we are simulating the scenario that happens when we do async shard fetching
     * from GatewaySerivce while we are finishing a recovery and concurrently clean files.
     * This should always be possible without any exception. Yet there was a bug where IndexShard
     * acquired the index writer lock before it called into the store that has it's own locking
     * for metadata reads
     */
    @Test
    public void testReadSnapshotConcurrently() throws Exception, InterruptedException {
        IndexShard indexShard = newStartedShard();
        indexDoc(indexShard, "0", "{}");
        if (randomBoolean()) {
            indexShard.refresh("test");
        }
        indexDoc(indexShard, "1", "{}");
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        final IndexShard newShard = reinitShard(indexShard);
        Store.MetadataSnapshot storeFileMetadatas = newShard.snapshotStoreMetadata();
        assertThat(storeFileMetadatas.size())
            .as("at least 2 files, commit and data: " + storeFileMetadatas.toString())
            .isGreaterThan(1);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        assertThatThrownBy(newShard::getEngine)
            .isExactlyInstanceOf(AlreadyClosedException.class); // no engine
        Thread thread = new Thread(() -> {
            latch.countDown();
            while (stop.get() == false) {
                try {
                    Store.MetadataSnapshot readMeta = newShard.snapshotStoreMetadata();
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).different.size()).isEqualTo(0);
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).missing.size()).isEqualTo(0);
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).identical.size())
                        .isEqualTo(storeFileMetadatas.size());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        });
        thread.start();
        latch.await();

        int iters = iterations(10, 100);
        for (int i = 0; i < iters; i++) {
            newShard.store().cleanupAndVerify("test", storeFileMetadatas);
        }
        assertThat(stop.compareAndSet(false, true)).isTrue();
        thread.join();
        closeShards(newShard);
    }

    @Test
    public void testIndexCheckOnStartup() throws Exception {
        IndexShard indexShard = newStartedShard(true);

        long numDocs = between(10, 100);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, Long.toString(i), "{}");
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        ShardPath shardPath = indexShard.shardPath();

        Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);
        CorruptionUtils.corruptIndex(random(), indexPath, false);

        AtomicInteger corruptedMarkerCount = new AtomicInteger();
         SimpleFileVisitor<Path> corruptedVisitor = new SimpleFileVisitor<>() {
             @Override
             public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                 if (Files.isRegularFile(file) && file.getFileName().toString().startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                     corruptedMarkerCount.incrementAndGet();
                 }
                 return FileVisitResult.CONTINUE;
             }
         };
        Files.walkFileTree(indexPath, corruptedVisitor);

        assertThat(corruptedMarkerCount.get()).as("corruption marker should not be there").isEqualTo(0);

        ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
            indexShard.routingEntry(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        // start shard and perform index check on startup. It enforce shard to fail due to corrupted index files
        IndexMetadata indexMetadata = IndexMetadata.builder(
            indexShard.indexSettings().getIndexMetadata()).settings(
                Settings.builder()
                    .put(indexShard.indexSettings.getSettings())
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom("true", "checksum")))
            .build();

        IndexShard corruptedShard = newShard(
            shardRouting,
            shardPath,
            indexMetadata,
            null,
            indexShard.engineFactoryProviders(),
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        assertThatThrownBy(() -> newStartedShard(p -> corruptedShard, true))
            .isExactlyInstanceOf(IndexShardRecoveryException.class)
            .hasMessage("failed recovery");

        // check that corrupt marker is there
        Files.walkFileTree(indexPath, corruptedVisitor);
        assertThat(corruptedMarkerCount.get()).as("store has to be marked as corrupted").isEqualTo(1);

        try {
            closeShards(corruptedShard);
        } catch (RuntimeException e) {
            // Ignored because corrupted shard can throw various exceptions on close
        }
    }

    @Test
    public void testShardDoesNotStartIfCorruptedMarkerIsPresent() throws Exception {
        IndexShard indexShard = newStartedShard(true);

        long numDocs = between(10, 100);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, Long.toString(i), "{}");
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        ShardPath shardPath = indexShard.shardPath();
        ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
            indexShard.routingEntry(), RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        IndexMetadata indexMetadata = indexShard.indexSettings().getIndexMetadata();

        Path indexPath = shardPath.getDataPath().resolve(ShardPath.INDEX_FOLDER_NAME);

        // create corrupted marker
        String corruptionMessage = "fake ioexception";
        try(Store store = createStore(indexShard.indexSettings(), shardPath)) {
            store.markStoreCorrupted(new IOException(corruptionMessage));
        }

        // try to start shard on corrupted files
        IndexShard corruptedShard = newShard(
            shardRouting,
            shardPath,
            indexMetadata,
            null,
            indexShard.engineFactoryProviders,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        assertThatThrownBy(() -> newStartedShard(p -> corruptedShard, true))
            .isExactlyInstanceOf(IndexShardRecoveryException.class)
            .cause()
                .hasMessage(corruptionMessage + " (resource=preexisting_corruption)");
        closeShards(corruptedShard);

        AtomicInteger corruptedMarkerCount = new AtomicInteger();
        SimpleFileVisitor<Path> corruptedVisitor = new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (Files.isRegularFile(file) && file.getFileName().toString().startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                    corruptedMarkerCount.incrementAndGet();
                }
                return FileVisitResult.CONTINUE;
            }
        };
        Files.walkFileTree(indexPath, corruptedVisitor);
        assertThat(corruptedMarkerCount.get()).as("store has to be marked as corrupted").isEqualTo(1);

        // try to start another time shard on corrupted files
        IndexShard corruptedShard2 = newShard(
            shardRouting,
            shardPath,
            indexMetadata,
            null,
            indexShard.engineFactoryProviders,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        assertThatThrownBy(() -> newStartedShard(p -> corruptedShard2, true))
            .isExactlyInstanceOf(IndexShardRecoveryException.class)
            .cause()
                .hasMessage(corruptionMessage + " (resource=preexisting_corruption)");
        closeShards(corruptedShard2);

        // check that corrupt marker is there
        corruptedMarkerCount.set(0);
        Files.walkFileTree(indexPath, corruptedVisitor);
        assertThat(corruptedMarkerCount.get()).as("store still has a single corrupt marker").isEqualTo(1);
    }

    /**
     * Simulates a scenario that happens when we are async fetching snapshot metadata from GatewayService
     * and checking index concurrently. This should always be possible without any exception.
     */
    @Test
    public void testReadSnapshotAndCheckIndexConcurrently() throws Exception {
        final boolean isPrimary = randomBoolean();
        IndexShard indexShard = newStartedShard(isPrimary);
        final long numDocs = between(10, 100);
        for (long i = 0; i < numDocs; i++) {
            indexDoc(indexShard, Long.toString(i), "{}");
            if (randomBoolean()) {
                indexShard.refresh("test");
            }
        }
        indexShard.flush(new FlushRequest());
        closeShards(indexShard);

        ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
            indexShard.routingEntry(),
            isPrimary
                ? RecoverySource.ExistingStoreRecoverySource.INSTANCE
                : RecoverySource.PeerRecoverySource.INSTANCE
        );
        IndexMetadata indexMetadata = IndexMetadata
            .builder(indexShard.indexSettings().getIndexMetadata())
            .settings(
                Settings.builder()
                    .put(indexShard.indexSettings.getSettings())
                    .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom("false", "true", "checksum")))
            .build();
        IndexShard newShard = newShard(
            shardRouting,
            indexShard.shardPath(),
            indexMetadata,
            null,
            indexShard.engineFactoryProviders,
            indexShard.getGlobalCheckpointSyncer(),
            indexShard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER
        );

        Store.MetadataSnapshot storeFileMetadatas = newShard.snapshotStoreMetadata();
        assertThat(storeFileMetadatas.size())
            .as("at least 2 files, commit and data: " + storeFileMetadatas.toString())
            .isGreaterThan(1);
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch latch = new CountDownLatch(1);
        Thread snapshotter = new Thread(() -> {
            latch.countDown();
            while (stop.get() == false) {
                try {
                    Store.MetadataSnapshot readMeta = newShard.snapshotStoreMetadata();
                    assertThat(readMeta.getNumDocs()).isEqualTo(numDocs);
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).different.size()).isEqualTo(0);
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).missing.size()).isEqualTo(0);
                    assertThat(storeFileMetadatas.recoveryDiff(readMeta).identical.size())
                        .isEqualTo(storeFileMetadatas.size());
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
        });
        snapshotter.start();

        if (isPrimary) {
            newShard.markAsRecovering(
                "store",
                new RecoveryState(
                    newShard.routingEntry(),
                    getFakeDiscoNode(newShard.routingEntry().currentNodeId()), null));
        } else {
            newShard.markAsRecovering(
                "peer",
                new RecoveryState(
                    newShard.routingEntry(),
                    getFakeDiscoNode(newShard.routingEntry().currentNodeId()),
                    getFakeDiscoNode(newShard.routingEntry().currentNodeId()))
            );
        }
        int iters = iterations(10, 100);
        latch.await();
        for (int i = 0; i < iters; i++) {
            newShard.checkIndex();
        }
        assertThat(stop.compareAndSet(false, true)).isEqualTo(true);
        snapshotter.join();
        closeShards(newShard);
    }

    @Test
    public void testIsSearchIdle() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        indexDoc(primary, "0", "{\"foo\" : \"bar\"}");
        assertThat(primary.getEngine().refreshNeeded()).isTrue();
        assertThat(primary.scheduledRefresh()).isTrue();
        assertThat(primary.isSearchIdle()).isFalse();

        IndexScopedSettings scopedSettings = primary.indexSettings().getScopedSettings();
        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO).build();
        scopedSettings.applySettings(settings);
        assertThat(primary.isSearchIdle()).isTrue();

        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.timeValueMinutes(1))
            .build();
        scopedSettings.applySettings(settings);
        assertThat(primary.isSearchIdle()).isFalse();

        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.timeValueMillis(10))
            .build();
        scopedSettings.applySettings(settings);

        assertBusy(() -> assertThat(primary.isSearchIdle()).isTrue());
        do {
            // now loop until we are fast enough... shouldn't take long
            primary.awaitShardSearchActive(aBoolean -> {});
        } while (primary.isSearchIdle());

        assertBusy(() -> assertThat(primary.isSearchIdle()).isTrue());
        do {
            // now loop until we are fast enough... shouldn't take long
            primary.acquireSearcher("test").close();
        } while (primary.isSearchIdle());
        closeShards(primary);
    }

    @Test
    public void testScheduledRefresh() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        indexDoc(primary, "0", "{\"foo\" : \"bar\"}");
        assertThat(primary.getEngine().refreshNeeded()).isEqualTo(true);
        assertThat(primary.scheduledRefresh()).isEqualTo(true);
        IndexScopedSettings scopedSettings = primary.indexSettings().getScopedSettings();
        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO).build();
        scopedSettings.applySettings(settings);

        assertThat(primary.getEngine().refreshNeeded()).isFalse();
        indexDoc(primary, "1", "{\"foo\" : \"bar\"}");
        assertThat(primary.getEngine().refreshNeeded()).isEqualTo(true);
        long lastSearchAccess = primary.getLastSearcherAccess();
        assertThat(primary.scheduledRefresh()).isEqualTo(false);
        assertThat(lastSearchAccess).isEqualTo(primary.getLastSearcherAccess());
        // wait until the thread-pool has moved the timestamp otherwise we can't assert on this below
        assertBusy(() -> assertThat(primary.getThreadPool().relativeTimeInMillis()).isGreaterThan(lastSearchAccess));
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            primary.awaitShardSearchActive(refreshed -> {
                assertThat(refreshed).isTrue();
                try (Engine.Searcher searcher = primary.acquireSearcher("test")) {
                    assertThat(searcher.getIndexReader().numDocs()).isEqualTo(2);
                } finally {
                    latch.countDown();
                }
            });
        }
        assertThat(lastSearchAccess)
            .as("awaitShardSearchActive must access a searcher to remove search idle state")
            .isNotEqualTo(primary.getLastSearcherAccess());
        assertThat(lastSearchAccess < primary.getLastSearcherAccess()).isTrue();
        try (Engine.Searcher searcher = primary.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs()).isEqualTo(1);
        }
        assertThat(primary.getEngine().refreshNeeded()).isEqualTo(true);
        assertThat(primary.scheduledRefresh()).isEqualTo(true);
        latch.await();
        CountDownLatch latch1 = new CountDownLatch(1);
        primary.awaitShardSearchActive(refreshed -> {
            assertThat(refreshed).isFalse();
            try (Engine.Searcher searcher = primary.acquireSearcher("test")) {
                assertThat(searcher.getIndexReader().numDocs()).isEqualTo(2);
            } finally {
                latch1.countDown();
            }

        });
        latch1.await();
        closeShards(primary);
    }

    @Test
    public void testRefreshIsNeededWithRefreshListeners() throws Exception, InterruptedException {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metadata = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard primary = newShard(new ShardId(metadata.getIndex(), 0), true, "n1", metadata, null);
        recoverShardFromStore(primary);
        indexDoc(primary, "0", "{\"foo\" : \"bar\"}");
        assertThat(primary.getEngine().refreshNeeded()).isTrue();
        assertThat(primary.scheduledRefresh()).isTrue();
        Engine.IndexResult doc = indexDoc(primary, "1", "{\"foo\" : \"bar\"}");
        CountDownLatch latch = new CountDownLatch(1);
        primary.addRefreshListener(doc.getTranslogLocation(), r -> latch.countDown());
        assertThat(latch.getCount()).isEqualTo(1L);
        assertThat(primary.getEngine().refreshNeeded()).isTrue();
        assertThat(primary.scheduledRefresh()).isTrue();
        latch.await();

        IndexScopedSettings scopedSettings = primary.indexSettings().getScopedSettings();
        settings = Settings.builder().put(settings).put(IndexSettings.INDEX_SEARCH_IDLE_AFTER.getKey(), TimeValue.ZERO).build();
        scopedSettings.applySettings(settings);

        doc = indexDoc(primary, "2", "{\"foo\" : \"bar\"}");
        CountDownLatch latch1 = new CountDownLatch(1);
        primary.addRefreshListener(doc.getTranslogLocation(), r -> latch1.countDown());
        assertThat(latch1.getCount()).isEqualTo(1L);
        assertThat(primary.getEngine().refreshNeeded()).isTrue();
        assertThat(primary.scheduledRefresh()).isTrue();
        latch1.await();
        closeShards(primary);
    }

    @Test
    public void testRecoveryFailsAfterMovingToRelocatedState() throws Exception {
        IndexShard shard = newStartedShard(true);
        ShardRouting origRouting = shard.routingEntry();
        assertThat(shard.state()).isEqualTo(IndexShardState.STARTED);
        ShardRouting inRecoveryRouting = ShardRoutingHelper.relocate(origRouting, "some_node");
        IndexShardTestCase.updateRoutingEntry(shard, inRecoveryRouting);
        shard.relocated(
            inRecoveryRouting.getTargetRelocatingShard().allocationId().getId(),
            primaryContext -> {
            });
        assertThat(shard.isRelocatedPrimary()).isTrue();
        try {
            IndexShardTestCase.updateRoutingEntry(shard, origRouting);
            fail("Expected IndexShardRelocatedException");
        } catch (IndexShardRelocatedException expected) {
        }

        closeShards(shard);
    }

    @Test
    public void testRejectOperationPermitWithHigherTermWhenNotStarted() throws Exception {
        IndexShard indexShard = newShard(false);
        assertThatThrownBy(() ->
            randomReplicaOperationPermitAcquisition(
                indexShard,
                indexShard.getPendingPrimaryTerm() + randomIntBetween(1, 100),
                UNASSIGNED_SEQ_NO,
                randomNonNegativeLong(),
                null,
                "")
        ).isExactlyInstanceOf(IndexShardNotStartedException.class);
        closeShards(indexShard);
    }

    @Test
    public void testPrimaryPromotionDelaysOperations() throws IOException, BrokenBarrierException, InterruptedException {
        IndexShard indexShard = newShard(false);
        recoveryEmptyReplica(indexShard, randomBoolean());

        int operations = scaledRandomIntBetween(1, 64);
        CyclicBarrier barrier = new CyclicBarrier(1 + operations);
        CountDownLatch latch = new CountDownLatch(operations);
        CountDownLatch operationLatch = new CountDownLatch(1);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < operations; i++) {
            final String id = "t_" + i;
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                indexShard.acquireReplicaOperationPermit(
                    indexShard.getPendingPrimaryTerm(),
                    indexShard.getLastKnownGlobalCheckpoint(),
                    indexShard.getMaxSeqNoOfUpdatesOrDeletes(),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            latch.countDown();
                            try {
                                operationLatch.await();
                            } catch (final InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            releasable.close();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    ThreadPool.Names.WRITE, id);
            });
            thread.start();
            threads.add(thread);
        }

        barrier.await();
        latch.await();

        ShardRouting replicaRouting = indexShard.routingEntry();
        promoteReplica(
            indexShard,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build());


        int delayedOperations = scaledRandomIntBetween(1, 64);
        CyclicBarrier delayedOperationsBarrier = new CyclicBarrier(1 + delayedOperations);
        CountDownLatch delayedOperationsLatch = new CountDownLatch(delayedOperations);
        AtomicLong counter = new AtomicLong();
        List<Thread> delayedThreads = new ArrayList<>();
        for (int i = 0; i < delayedOperations; i++) {
            String id = "d_" + i;
            Thread thread = new Thread(() -> {
                try {
                    delayedOperationsBarrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                indexShard.acquirePrimaryOperationPermit(
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Releasable releasable) {
                            counter.incrementAndGet();
                            releasable.close();
                            delayedOperationsLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            throw new RuntimeException(e);
                        }
                    },
                    ThreadPool.Names.WRITE, id);
            });
            thread.start();
            delayedThreads.add(thread);
        }

        delayedOperationsBarrier.await();

        assertThat(counter.get()).isEqualTo(0L);

        operationLatch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        delayedOperationsLatch.await();
        assertThat(counter.get()).isEqualTo((long) delayedOperations);

        for (Thread thread : delayedThreads) {
            thread.join();
        }

        closeShards(indexShard);
    }

    /*
     * This test makes sure that people can use the shard routing entry + take an
     * operation permit to check whether a shard was already promoted to a primary.
     */
    @Test
    public void testPublishingOrderOnPromotion() throws IOException, InterruptedException, BrokenBarrierException {
        IndexShard indexShard = newShard(false);
        recoveryEmptyReplica(indexShard, randomBoolean());
        long promotedTerm = indexShard.getPendingPrimaryTerm() + 1;
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicBoolean stop = new AtomicBoolean();
        Thread thread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            while (stop.get() == false) {
                if (indexShard.routingEntry().primary()) {
                    assertThat(indexShard.getPendingPrimaryTerm()).isEqualTo(promotedTerm);
                    final PlainActionFuture<Releasable> permitAcquiredFuture = new PlainActionFuture<>();
                    indexShard.acquirePrimaryOperationPermit(permitAcquiredFuture, ThreadPool.Names.SAME, "bla");
                    try (Releasable ignored = FutureUtils.get(permitAcquiredFuture)) {
                        assertThat(indexShard.getReplicationGroup()).isNotNull();
                    }
                }
            }
        });
        thread.start();

        barrier.await();
        final ShardRouting replicaRouting = indexShard.routingEntry();
        promoteReplica(
            indexShard,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build());

        stop.set(true);
        thread.join();
        closeShards(indexShard);
    }

    @Test
    public void testPrimaryFillsSeqNoGapsOnPromotion() throws Exception {
        IndexShard indexShard = newShard(false);
        recoveryEmptyReplica(indexShard, randomBoolean());

        // most of the time this is large enough that most of the time there will be at least one gap
        int operations = 1024 - scaledRandomIntBetween(0, 1024);
        Result result = indexOnReplicaWithGaps(
            indexShard,
            operations,
            Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED),
            true);
        int maxSeqNo = result.maxSeqNo;

        // promote the replica
        ShardRouting replicaRouting = indexShard.routingEntry();
        promoteReplica(
            indexShard,
            Collections.singleton(replicaRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(replicaRouting.shardId()).addShard(replicaRouting).build());

        /*
         * This operation completing means that the delay operation executed as part
         *  of increasing the primary term has completed and the gaps are filled.
         */
        CountDownLatch latch = new CountDownLatch(1);
        indexShard.acquirePrimaryOperationPermit(
            new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            },
            ThreadPool.Names.GENERIC, "");

        latch.await();
        assertThat(indexShard.getLocalCheckpoint()).isEqualTo((long) maxSeqNo);
        closeShards(indexShard);
    }

    @Test
    public void testPrimaryPromotionRollsGeneration() throws Exception {
        IndexShard indexShard = newStartedShard(false);
        long currentTranslogGeneration = getTranslog(indexShard)
            .getGeneration()
            .translogFileGeneration;

        // promote the replica
        ShardRouting replicaRouting = indexShard.routingEntry();
        long newPrimaryTerm = indexShard.getPendingPrimaryTerm() + between(1, 10000);
        ShardRouting primaryRouting =
            newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                null,
                true,
                ShardRoutingState.STARTED,
                replicaRouting.allocationId());
        indexShard.updateShardState(
            primaryRouting,
            newPrimaryTerm,
            (shard, listener) -> { },
            0L,
            Collections.singleton(primaryRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(primaryRouting.shardId()).addShard(primaryRouting).build());

        /*
         * This operation completing means that the delay operation executed as
         *  part of increasing the primary term has completed and the
         * translog generation has rolled.
         */
        CountDownLatch latch = new CountDownLatch(1);
        indexShard.acquirePrimaryOperationPermit(
            new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new RuntimeException(e);
                }
            },
            ThreadPool.Names.GENERIC, "");

        latch.await();
        assertThat(
            getTranslog(indexShard).getGeneration().translogFileGeneration)
            .isEqualTo(currentTranslogGeneration + 1);
        assertThat(
            TestTranslog.getCurrentTerm(getTranslog(indexShard)))
            .isEqualTo(newPrimaryTerm);

        closeShards(indexShard);
    }

    @Test
    public void testOperationPermitsOnPrimaryShards() throws Exception {
        ShardId shardId = new ShardId("test", "_na_", 0);
        IndexShard indexShard;
        boolean isPrimaryMode;
        if (randomBoolean()) {
            // relocation target
            indexShard = newShard(newShardRouting(
                shardId,
                "local_node",
                "other node",
                true,
                ShardRoutingState.INITIALIZING,
                AllocationId.newRelocation(AllocationId.newInitializing())));
            assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
            isPrimaryMode = false;
        } else if (randomBoolean()) {
            // simulate promotion
            indexShard = newStartedShard(false);
            ShardRouting replicaRouting = indexShard.routingEntry();
            ShardRouting primaryRouting = newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                null,
                true,
                ShardRoutingState.STARTED,
                replicaRouting.allocationId());
            long newPrimaryTerm = indexShard.getPendingPrimaryTerm() + between(1, 1000);
            CountDownLatch latch = new CountDownLatch(1);
            indexShard.updateShardState(
                primaryRouting,
                newPrimaryTerm,
                (shard, listener) -> {
                    assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard))).isEqualTo(newPrimaryTerm);
                    latch.countDown();
                },
                0L,
                Collections.singleton(indexShard.routingEntry().allocationId().getId()),
                new IndexShardRoutingTable.Builder(indexShard.shardId()).addShard(primaryRouting).build());
            latch.await();
            assertThat(indexShard.getActiveOperationsCount()).isIn(0, IndexShard.OPERATIONS_BLOCKED);
            if (randomBoolean()) {
                assertBusy(() -> assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0));
            }
            isPrimaryMode = true;
        } else {
            indexShard = newStartedShard(true);
            assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
            isPrimaryMode = true;
        }
        assert indexShard.getReplicationTracker().isPrimaryMode() == isPrimaryMode;
        long pendingPrimaryTerm = indexShard.getPendingPrimaryTerm();
        if (isPrimaryMode) {
            Releasable operation1 = acquirePrimaryOperationPermitBlockingly(indexShard);
            assertThat(indexShard.getActiveOperationsCount()).isEqualTo(1);
            Releasable operation2 = acquirePrimaryOperationPermitBlockingly(indexShard);
            assertThat(indexShard.getActiveOperationsCount()).isEqualTo(2);

            Releasables.close(operation1, operation2);
            assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
        } else {
            indexShard.acquirePrimaryOperationPermit(
                new ActionListener<>() {
                    @Override
                    public void onResponse(final Releasable releasable) {
                        throw new AssertionError();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        assertThat(e)
                            .isExactlyInstanceOf(ShardNotInPrimaryModeException.class)
                            .hasMessageContaining("shard is not in primary mode");
                    }
                },
                ThreadPool.Names.SAME,
                "test");

            CountDownLatch latch = new CountDownLatch(1);
            indexShard.acquireAllPrimaryOperationsPermits(
                new ActionListener<>() {
                    @Override
                    public void onResponse(final Releasable releasable) {
                        throw new AssertionError();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        assertThat(e).isExactlyInstanceOf(ShardNotInPrimaryModeException.class)
                            .hasMessageContaining("shard is not in primary mode");
                        latch.countDown();
                    }
                },
                TimeValue.timeValueSeconds(30));
            latch.await();
        }

        if (Assertions.ENABLED && indexShard.routingEntry().isRelocationTarget() == false) {
            assertThatThrownBy(
                () -> indexShard.acquireReplicaOperationPermit(
                        pendingPrimaryTerm,
                        indexShard.getLastKnownGlobalCheckpoint(),
                        indexShard.getMaxSeqNoOfUpdatesOrDeletes(),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Releasable releasable) {
                                fail();
                            }

                            @Override
                            public void onFailure(Exception e) {
                                throw Exceptions.toRuntimeException(e);
                            }
                        },
                        ThreadPool.Names.WRITE,
                        ""
                    ))
                .hasCauseInstanceOf(AssertionError.class)
                .hasMessageContaining("in primary mode cannot be a replication target");
        }

        closeShards(indexShard);
    }

    @Test
    public void testRecoverFromStoreWithOutOfOrderDelete() throws IOException {
        /*
         * The flow of this test:
         * - delete #1
         * - roll generation (to create gen 2)
         * - index #0
         * - index #3
         * - flush (commit point has max_seqno 3, and local checkpoint 1 -> points at gen 2, previous commit point is maintained)
         * - index #2
         * - index #5
         * - If flush and then recover from the existing store, delete #1 will be removed while index #0 is still retained and replayed.
         */
        final IndexShard shard = newStartedShard(false);
        long primaryTerm = shard.getOperationPrimaryTerm();
        shard.advanceMaxSeqNoOfUpdatesOrDeletes(1); // manually advance msu for this delete
        shard.applyDeleteOperationOnReplica(1, primaryTerm, 2, "id");
        shard.getEngine().rollTranslogGeneration(); // isolate the delete in it's own generation
        shard.applyIndexOperationOnReplica(0, primaryTerm, 1, Translog.UNSET_AUTO_GENERATED_TIMESTAMP, false,
            new SourceToParse(shard.shardId().getIndexName(), "id", new BytesArray("{}"), XContentType.JSON));
        shard.applyIndexOperationOnReplica(3, primaryTerm, 3, Translog.UNSET_AUTO_GENERATED_TIMESTAMP, false,
            new SourceToParse(shard.shardId().getIndexName(), "id-3", new BytesArray("{}"), XContentType.JSON));
        // Flushing a new commit with local checkpoint=1 allows to skip the translog gen #1 in recovery.
        shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
        shard.applyIndexOperationOnReplica(2, primaryTerm, 3, Translog.UNSET_AUTO_GENERATED_TIMESTAMP, false,
            new SourceToParse(shard.shardId().getIndexName(), "id-2", new BytesArray("{}"), XContentType.JSON));
        shard.applyIndexOperationOnReplica(5, primaryTerm, 1, Translog.UNSET_AUTO_GENERATED_TIMESTAMP, false,
            new SourceToParse(shard.shardId().getIndexName(), "id-5", new BytesArray("{}"), XContentType.JSON));
        shard.sync(); // advance local checkpoint

        final int translogOps;
        final int replayedOps;
        if (randomBoolean()) {
            // Advance the global checkpoint to remove the 1st commit; this shard will recover the 2nd commit.
            shard.updateGlobalCheckpointOnReplica(3, "test");
            logger.info("--> flushing shard");
            shard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            translogOps = 4; // delete #1 won't be replayed.
            replayedOps = 3;
        } else {
            if (randomBoolean()) {
                shard.getEngine().rollTranslogGeneration();
            }
            translogOps = 5;
            replayedOps = 5;
        }

        final ShardRouting replicaRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(shard,
            newShardRouting(replicaRouting.shardId(), replicaRouting.currentNodeId(), true, ShardRoutingState.INITIALIZING,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE));
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertThat(recoverFromStore(newShard)).isTrue();
        assertThat(replayedOps).isEqualTo(newShard.recoveryState().getTranslog().recoveredOperations());
        assertThat(translogOps).isEqualTo(newShard.recoveryState().getTranslog().totalOperations());
        assertThat(translogOps).isEqualTo(newShard.recoveryState().getTranslog().totalOperationsOnStart());
        updateRoutingEntry(newShard, ShardRoutingHelper.moveToStarted(newShard.routingEntry()));
        assertDocCount(newShard, 3);
        closeShards(newShard);
    }

    @Test
    public void testRecoverFromStalePrimaryForceNewHistoryUUID() throws IOException {
        final IndexShard shard = newStartedShard(true);
        int totalOps = randomInt(10);
        for (int i = 0; i < totalOps; i++) {
            indexDoc(shard, "_doc", Integer.toString(i));
        }
        if (randomBoolean()) {
            shard.updateLocalCheckpointForShard(shard.shardRouting.allocationId().getId(), totalOps - 1);
            flushShard(shard);
        }

        closeShards(shard);
    }

    @Test
    public void testOperationPermitOnReplicaShards() throws Exception {
        ShardId shardId = new ShardId("test", "_na_", 0);
        IndexShard indexShard;
        boolean engineClosed;
        switch (randomInt(2)) {
            case 0:
                // started replica
                indexShard = newStartedShard(false);
                engineClosed = false;
                break;
            case 1: {
                // initializing replica / primary
                boolean relocating = randomBoolean();
                ShardRouting routing = newShardRouting(
                    shardId, "local_node",
                    relocating ? "sourceNode" : null,
                    relocating && randomBoolean(),
                    ShardRoutingState.INITIALIZING,
                    relocating
                        ? AllocationId.newRelocation(AllocationId.newInitializing())
                        : AllocationId.newInitializing());
                indexShard = newShard(routing);
                engineClosed = true;
                break;
            }
            case 2: {
                // relocation source
                indexShard = newStartedShard(true);
                ShardRouting routing = indexShard.routingEntry();
                routing = newShardRouting(
                    routing.shardId(),
                    routing.currentNodeId(),
                    "otherNode",
                    true,
                    ShardRoutingState.RELOCATING,
                    AllocationId.newRelocation(routing.allocationId()));
                IndexShardTestCase.updateRoutingEntry(indexShard, routing);
                indexShard.relocated(routing.getTargetRelocatingShard().allocationId().getId(), primaryContext -> {
                });
                engineClosed = false;
                break;
            }
            default:
                throw new UnsupportedOperationException("get your numbers straight");

        }
        ShardRouting shardRouting = indexShard.routingEntry();
        logger.info("shard routing to {}", shardRouting);

        assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
        if (shardRouting.primary() == false && Assertions.ENABLED) {
            assertThatThrownBy(() -> indexShard.acquirePrimaryOperationPermit(null, ThreadPool.Names.WRITE, ""))
                .isExactlyInstanceOf(AssertionError.class)
                .hasMessageContaining("acquirePrimaryOperationPermit should only be called on primary shard");

            assertThatThrownBy(() -> indexShard.acquireAllPrimaryOperationsPermits(
                    null,
                    TimeValue.timeValueSeconds(30L))
                ).isExactlyInstanceOf(AssertionError.class)
                .hasMessageContaining("acquireAllPrimaryOperationsPermits should only be called on primary shard");
        }

        long primaryTerm = indexShard.getPendingPrimaryTerm();
        long translogGen = engineClosed ? -1 : getTranslog(indexShard).getGeneration().translogFileGeneration;

        Releasable operation1;
        Releasable operation2;
        if (engineClosed == false) {
            operation1 = acquireReplicaOperationPermitBlockingly(indexShard, primaryTerm);
            assertThat(1).isEqualTo(indexShard.getActiveOperationsCount());
            operation2 = acquireReplicaOperationPermitBlockingly(indexShard, primaryTerm);
            assertThat(2).isEqualTo(indexShard.getActiveOperationsCount());
        } else {
            operation1 = null;
            operation2 = null;
        }
        {
            AtomicBoolean onResponse = new AtomicBoolean();
            AtomicReference<Exception> onFailure = new AtomicReference<>();
            CyclicBarrier barrier = new CyclicBarrier(2);
            long newPrimaryTerm = primaryTerm + 1 + randomInt(20);
            if (engineClosed == false) {
                assertThat(indexShard.getLocalCheckpoint()).isEqualTo(SequenceNumbers.NO_OPS_PERFORMED);
                assertThat(indexShard.getLastKnownGlobalCheckpoint()).isEqualTo(SequenceNumbers.NO_OPS_PERFORMED);
            }
            long newGlobalCheckPoint;
            if (engineClosed || randomBoolean()) {
                newGlobalCheckPoint = SequenceNumbers.NO_OPS_PERFORMED;
            } else {
                long localCheckPoint = indexShard.getLastKnownGlobalCheckpoint() + randomInt(100);
                // advance local checkpoint
                for (int i = 0; i <= localCheckPoint; i++) {
                    indexShard.markSeqNoAsNoop(
                        i,
                        indexShard.getOperationPrimaryTerm(),
                        "dummy doc"
                    );
                }
                indexShard.sync(); // advance local checkpoint
                newGlobalCheckPoint = randomIntBetween(
                    (int) indexShard.getLastKnownGlobalCheckpoint(),
                    (int) localCheckPoint);
            }
            long expectedLocalCheckpoint;
            if (newGlobalCheckPoint == UNASSIGNED_SEQ_NO) {
                expectedLocalCheckpoint = SequenceNumbers.NO_OPS_PERFORMED;
            } else {
                expectedLocalCheckpoint = newGlobalCheckPoint;
            }
            // but you can not increment with a new primary term until the operations on the older primary term complete
            Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                ActionListener<Releasable> listener = new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        assertThat(indexShard.getPendingPrimaryTerm()).isEqualTo(newPrimaryTerm);
                        assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard))).isEqualTo(newPrimaryTerm);
                        assertThat(indexShard.getLocalCheckpoint()).isEqualTo(expectedLocalCheckpoint);
                        assertThat(indexShard.getLastKnownGlobalCheckpoint()).isEqualTo(newGlobalCheckPoint);
                        onResponse.set(true);
                        releasable.close();
                        finish();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailure.set(e);
                        finish();
                    }

                    private void finish() {
                        try {
                            barrier.await();
                        } catch (final BrokenBarrierException | InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };
                try {
                    randomReplicaOperationPermitAcquisition(
                        indexShard,
                        newPrimaryTerm,
                        newGlobalCheckPoint,
                        randomNonNegativeLong(),
                        listener,
                        "");
                } catch (Exception e) {
                    listener.onFailure(e);
                }
            });
            thread.start();
            barrier.await();
            if (indexShard.state() == IndexShardState.CREATED || indexShard.state() == IndexShardState.RECOVERING) {
                barrier.await();
                assertThat(indexShard.getPendingPrimaryTerm()).isEqualTo(primaryTerm);
                assertThat(onResponse.get()).isEqualTo(false);
                assertThat(onFailure.get()).isExactlyInstanceOf(IndexShardNotStartedException.class);
                Releasables.close(operation1);
                Releasables.close(operation2);
            } else {
                // our operation should be blocked until the previous operations complete
                assertThat(onResponse.get()).isEqualTo(false);
                assertThat(onFailure.get()).isNull();
                assertThat(indexShard.getOperationPrimaryTerm()).isEqualTo(primaryTerm);
                assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard))).isEqualTo(primaryTerm);
                Releasables.close(operation1);
                // our operation should still be blocked
                assertThat(onResponse.get()).isEqualTo(false);
                assertThat(onFailure.get()).isNull();
                assertThat(indexShard.getOperationPrimaryTerm()).isEqualTo(primaryTerm);
                assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard))).isEqualTo(primaryTerm);
                Releasables.close(operation2);
                barrier.await();
                // now lock acquisition should have succeeded
                assertThat(indexShard.getOperationPrimaryTerm()).isEqualTo(newPrimaryTerm);
                assertThat(indexShard.getPendingPrimaryTerm()).isEqualTo(newPrimaryTerm);
                assertThat(TestTranslog.getCurrentTerm(getTranslog(indexShard))).isEqualTo(newPrimaryTerm);
                if (engineClosed) {
                    assertThat(onResponse.get()).isEqualTo(false);
                    assertThat(onFailure.get()).isExactlyInstanceOf(AlreadyClosedException.class);
                } else {
                    assertThat(onResponse.get()).isEqualTo(true);
                    assertThat(onFailure.get()).isNull();
                    assertThat(getTranslog(indexShard).getGeneration().translogFileGeneration)
                        // if rollback happens we roll translog twice: one when we flush
                        // a commit before opening a read-only engine and one after replaying
                        // translog (upto the global checkpoint); otherwise we roll translog once.
                        .satisfiesAnyOf(
                            x -> assertThat(x).isEqualTo(translogGen + 1),
                            x -> assertThat(x).isEqualTo(translogGen + 2)
                        );
                    assertThat(indexShard.getLocalCheckpoint()).isEqualTo(expectedLocalCheckpoint);
                    assertThat(indexShard.getLastKnownGlobalCheckpoint()).isEqualTo(newGlobalCheckPoint);
                }
            }
            thread.join();
            assertThat(indexShard.getActiveOperationsCount()).isEqualTo(0);
        }

        {
            CountDownLatch latch = new CountDownLatch(1);
            AtomicBoolean onResponse = new AtomicBoolean();
            AtomicBoolean onFailure = new AtomicBoolean();
            AtomicReference<Exception> onFailureException = new AtomicReference<>();
            ActionListener<Releasable> onLockAcquired = new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable releasable) {
                    onResponse.set(true);
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    onFailure.set(true);
                    onFailureException.set(e);
                    latch.countDown();
                }
            };

            long oldPrimaryTerm = indexShard.getPendingPrimaryTerm() - 1;
            randomReplicaOperationPermitAcquisition(
                indexShard,
                oldPrimaryTerm,
                indexShard.getLastKnownGlobalCheckpoint(),
                randomNonNegativeLong(),
                onLockAcquired,
                "");
            latch.await();
            assertThat(onResponse.get()).isEqualTo(false);
            assertThat(onFailure.get()).isEqualTo(true);
            assertThat(onFailureException.get()).isExactlyInstanceOf(IllegalStateException.class);
            assertThat(onFailureException.get())
                .hasMessageContaining("operation primary term [" + oldPrimaryTerm + "] is too old");
        }
        closeShard(indexShard, false);
        // skip asserting translog and Lucene as we rolled back Lucene but did not execute resync
    }

    @Test
    public void testAcquireReplicaPermitAdvanceMaxSeqNoOfUpdates() throws Exception {
        IndexShard replica = newStartedShard(false);
        assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(SequenceNumbers.NO_OPS_PERFORMED);
        long currentMaxSeqNoOfUpdates = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        replica.advanceMaxSeqNoOfUpdatesOrDeletes(currentMaxSeqNoOfUpdates);

        long newMaxSeqNoOfUpdates = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        randomReplicaOperationPermitAcquisition(
            replica,
            replica.getOperationPrimaryTerm(),
            replica.getLastKnownGlobalCheckpoint(),
            newMaxSeqNoOfUpdates,
            fut,
            "");
        try (Releasable ignored = FutureUtils.get(fut)) {
            assertThat(replica.getMaxSeqNoOfUpdatesOrDeletes())
                .isEqualTo(Math.max(currentMaxSeqNoOfUpdates, newMaxSeqNoOfUpdates));
        }
        closeShards(replica);
    }

    @Test
    public void testGlobalCheckpointSync() throws IOException {
        // create the primary shard with a callback that sets a boolean when the global checkpoint sync is invoked
        ShardId shardId = new ShardId("index", "_na_", 0);
        ShardRouting shardRouting =
            TestShardRouting.newShardRouting(
                shardId,
                randomAlphaOfLength(8),
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata.Builder indexMetadata = IndexMetadata
            .builder(shardRouting.getIndexName())
            .settings(settings).primaryTerm(0, 1);
        AtomicBoolean synced = new AtomicBoolean();
        IndexShard primaryShard = newShard(
            shardRouting,
            indexMetadata.build(),
            List.of(),
            () -> synced.set(true)
        );
        // add a replica
        recoverShardFromStore(primaryShard);
        IndexShard replicaShard = newShard(shardId, false);
        recoverReplica(replicaShard, primaryShard, true);
        int maxSeqNo = randomIntBetween(0, 128);
        for (int i = 0; i <= maxSeqNo; i++) {
            EngineTestCase.generateNewSeqNo(primaryShard.getEngine());
        }
        long checkpoint = rarely() ? maxSeqNo - scaledRandomIntBetween(0, maxSeqNo) : maxSeqNo;

        // set up local checkpoints on the shard copies
        primaryShard.updateLocalCheckpointForShard(shardRouting.allocationId().getId(), checkpoint);
        int replicaLocalCheckpoint = randomIntBetween(0, Math.toIntExact(checkpoint));
        String replicaAllocationId = replicaShard.routingEntry().allocationId().getId();
        primaryShard.updateLocalCheckpointForShard(replicaAllocationId, replicaLocalCheckpoint);

        // initialize the local knowledge on the primary of the persisted global
        // checkpoint on the replica shard
        int replicaGlobalCheckpoint = randomIntBetween(
            Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED),
            Math.toIntExact(primaryShard.getLastKnownGlobalCheckpoint()));
        primaryShard.updateGlobalCheckpointForShard(replicaAllocationId, replicaGlobalCheckpoint);

        // initialize the local knowledge on the primary of the persisted global checkpoint on the primary
        primaryShard.updateGlobalCheckpointForShard(
            shardRouting.allocationId().getId(),
            primaryShard.getLastKnownGlobalCheckpoint());

        // simulate a background maybe sync; it should only run if the knowledge on the replica
        // of the global checkpoint lags the primary
        primaryShard.maybeSyncGlobalCheckpoint("test");
        assertThat(synced.get())
            .isEqualTo(maxSeqNo == primaryShard.getLastKnownGlobalCheckpoint()
               && (replicaGlobalCheckpoint < checkpoint));

        // simulate that the background sync advanced the global checkpoint on the replica
        primaryShard.updateGlobalCheckpointForShard(
            replicaAllocationId,
            primaryShard.getLastKnownGlobalCheckpoint()
        );

        // reset our boolean so that we can assert after another simulated maybe sync
        synced.set(false);

        primaryShard.maybeSyncGlobalCheckpoint("test");

        // this time there should not be a sync since all the replica copies are caught up with the primary
        assertThat(synced.get()).isEqualTo(false);

        closeShards(replicaShard, primaryShard);
    }

    @Test
    public void testRestoreLocalHistoryFromTranslogOnPromotion() throws IOException, InterruptedException {
        IndexShard indexShard = newStartedShard(false);
        int operations = 1024 - scaledRandomIntBetween(0, 1024);
        indexOnReplicaWithGaps(indexShard, operations, Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), true);

        long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();
        long globalCheckpointOnReplica = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        indexShard.updateGlobalCheckpointOnReplica(globalCheckpointOnReplica, "test");

        long globalCheckpoint = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        final long maxSeqNoOfUpdatesOrDeletes = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, maxSeqNo);
        final long maxSeqNoOfUpdatesOrDeletesBeforeRollback = indexShard.getMaxSeqNoOfUpdatesOrDeletes();
        Set<String> docsBeforeRollback = getShardDocUIDs(indexShard);
        CountDownLatch latch = new CountDownLatch(1);
        randomReplicaOperationPermitAcquisition(
            indexShard,
            indexShard.getPendingPrimaryTerm() + 1,
            globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes,
            new ActionListener<>() {
                @Override
                public void onResponse(Releasable releasable) {
                    releasable.close();
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {

                }
            }, "");

        latch.await();
        long globalCheckpointOnPromotedReplica = Math.max(globalCheckpointOnReplica, globalCheckpoint);
        long expectedMaxSeqNoOfUpdatesOrDeletes = globalCheckpointOnPromotedReplica < maxSeqNo
            ? maxSeqNo
            : Math.max(maxSeqNoOfUpdatesOrDeletesBeforeRollback, maxSeqNoOfUpdatesOrDeletes);
        assertThat(indexShard.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(expectedMaxSeqNoOfUpdatesOrDeletes);
        final ShardRouting newRouting = indexShard.routingEntry().moveActiveReplicaToPrimary();
        final CountDownLatch resyncLatch = new CountDownLatch(1);
        indexShard.updateShardState(
            newRouting,
            indexShard.getPendingPrimaryTerm() + 1,
            (s, r) -> resyncLatch.countDown(),
            1L,
            Collections.singleton(newRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(newRouting.shardId()).addShard(newRouting).build());
        resyncLatch.await();
        assertThat(indexShard.getLocalCheckpoint()).isEqualTo(maxSeqNo);
        assertThat(indexShard.seqNoStats().getMaxSeqNo()).isEqualTo(maxSeqNo);
        assertThat(getShardDocUIDs(indexShard)).isEqualTo(docsBeforeRollback);
        assertThat(indexShard.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(expectedMaxSeqNoOfUpdatesOrDeletes);
        closeShard(indexShard, false);
    }

    public void testRollbackReplicaEngineOnPromotion() throws IOException, InterruptedException {
        final IndexShard indexShard = newStartedShard(false);

        // most of the time this is large enough that most of the time there will be at least one gap
        final int operations = 1024 - scaledRandomIntBetween(0, 1024);
        indexOnReplicaWithGaps(indexShard, operations, Math.toIntExact(SequenceNumbers.NO_OPS_PERFORMED), true);

        final long globalCheckpointOnReplica = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        indexShard.updateGlobalCheckpointOnReplica(globalCheckpointOnReplica, "test");
        final long globalCheckpoint = randomLongBetween(UNASSIGNED_SEQ_NO, indexShard.getLocalCheckpoint());
        Set<String> docsBelowGlobalCheckpoint = getShardDocUIDs(indexShard).stream()
            .filter(id -> Long.parseLong(id) <= Math.max(globalCheckpointOnReplica, globalCheckpoint)).collect(Collectors.toSet());
        final CountDownLatch latch = new CountDownLatch(1);
        final boolean shouldRollback = Math.max(globalCheckpoint, globalCheckpointOnReplica) < indexShard.seqNoStats().getMaxSeqNo()
                                       && indexShard.seqNoStats().getMaxSeqNo() != SequenceNumbers.NO_OPS_PERFORMED;
        final Engine beforeRollbackEngine = indexShard.getEngine();
        final long newMaxSeqNoOfUpdates = randomLongBetween(indexShard.getMaxSeqNoOfUpdatesOrDeletes(), Long.MAX_VALUE);
        randomReplicaOperationPermitAcquisition(indexShard,
                                                indexShard.getPendingPrimaryTerm() + 1,
                                                globalCheckpoint,
                                                newMaxSeqNoOfUpdates,
                                                new ActionListener<Releasable>() {
                                                    @Override
                                                    public void onResponse(final Releasable releasable) {
                                                        releasable.close();
                                                        latch.countDown();
                                                    }

                                                    @Override
                                                    public void onFailure(final Exception e) {

                                                    }
                                                }, "");

        latch.await();
        if (globalCheckpointOnReplica == UNASSIGNED_SEQ_NO && globalCheckpoint == UNASSIGNED_SEQ_NO) {
            assertThat(indexShard.getLocalCheckpoint()).isEqualTo(SequenceNumbers.NO_OPS_PERFORMED);
        } else {
            assertThat(indexShard.getLocalCheckpoint()).isEqualTo(Math.max(globalCheckpoint, globalCheckpointOnReplica));
        }
        assertThat(getShardDocUIDs(indexShard)).isEqualTo(docsBelowGlobalCheckpoint);
        if (shouldRollback) {
            assertThat(indexShard.getEngine()).isNotSameAs(beforeRollbackEngine);
        } else {
            assertThat(indexShard.getEngine()).isSameAs(beforeRollbackEngine);
        }
        assertThat(indexShard.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(newMaxSeqNoOfUpdates);
        // ensure that after the local checkpoint throw back and indexing again, the local checkpoint advances
        final Result result = indexOnReplicaWithGaps(indexShard, operations, Math.toIntExact(indexShard.getLocalCheckpoint()), true);
        assertThat(indexShard.getLocalCheckpoint()).isEqualTo((long) result.localCheckpoint);
        closeShard(indexShard, false);
    }

    /**
     * Randomizes the usage of
     * {@link IndexShard#acquireReplicaOperationPermit(long, long, long, ActionListener, String, Object)}
     * and{@link IndexShard#acquireAllReplicaOperationsPermits(long, long, long, ActionListener, TimeValue)}
     * in order to acquire a permit.
     */
    private void randomReplicaOperationPermitAcquisition(IndexShard indexShard,
                                                         long opPrimaryTerm,
                                                         long globalCheckpoint,
                                                         long maxSeqNoOfUpdatesOrDeletes,
                                                         ActionListener<Releasable> listener,
                                                         String info) {
        if (randomBoolean()) {
            String executor = ThreadPool.Names.WRITE;
            indexShard.acquireReplicaOperationPermit(
                opPrimaryTerm,
                globalCheckpoint,
                maxSeqNoOfUpdatesOrDeletes,
                listener,
                executor,
                info);
        } else {
            TimeValue timeout = TimeValue.timeValueSeconds(30L);
            indexShard.acquireAllReplicaOperationsPermits(
                opPrimaryTerm,
                globalCheckpoint,
                maxSeqNoOfUpdatesOrDeletes,
                listener,
                timeout);
        }
    }

    @Test
    public void testConcurrentTermIncreaseOnReplicaShard() throws Exception {
        IndexShard indexShard = newStartedShard(false);

        CyclicBarrier barrier = new CyclicBarrier(3);
        CountDownLatch latch = new CountDownLatch(2);

        long primaryTerm = indexShard.getPendingPrimaryTerm();
        AtomicLong counter = new AtomicLong();
        AtomicReference<Exception> onFailure = new AtomicReference<>();

        LongFunction<Runnable> function = increment -> () -> {
            assert increment > 0;
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            indexShard.acquireReplicaOperationPermit(
                primaryTerm + increment,
                indexShard.getLastKnownGlobalCheckpoint(),
                randomNonNegativeLong(),
                new ActionListener<Releasable>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        counter.incrementAndGet();
                        assertThat(indexShard.getOperationPrimaryTerm()).isEqualTo(primaryTerm + increment);
                        latch.countDown();
                        releasable.close();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        onFailure.set(e);
                        latch.countDown();
                    }
                },
                ThreadPool.Names.WRITE, "");
        };

        long firstIncrement = 1 + (randomBoolean() ? 0 : 1);
        long secondIncrement = 1 + (randomBoolean() ? 0 : 1);
        Thread first = new Thread(function.apply(firstIncrement));
        Thread second = new Thread(function.apply(secondIncrement));

        first.start();
        second.start();

        // the two threads synchronize attempting to acquire an operation permit
        barrier.await();

        // we wait for both operations to complete
        latch.await();

        first.join();
        second.join();

        Exception e;
        if ((e = onFailure.get()) != null) {
            /*
             * If one thread tried to set the primary term to a higher value than the other thread and the thread with the higher term won
             * the race, then the other thread lost the race and only one operation should have been executed.
             */
            assertThat(e)
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessageFindingMatch("operation primary term \\[\\d+\\] is too old");
            assertThat(counter.get()).isEqualTo(1L);
        } else {
            assertThat(counter.get()).isEqualTo(2L);
        }

        assertThat(indexShard.getPendingPrimaryTerm())
            .isEqualTo(primaryTerm + Math.max(firstIncrement, secondIncrement));
        assertThat(indexShard.getOperationPrimaryTerm()).isEqualTo(indexShard.getPendingPrimaryTerm());

        closeShards(indexShard);
    }

    /*
     * test one can snapshot the store at various lifecycle stages
     */
    @Test
    public void testSnapshotStore() throws IOException {
        final IndexShard shard = newStartedShard(true);
        indexDoc(shard, "0");
        flushShard(shard);

        final IndexShard newShard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode(
            "foo",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT);

        Store.MetadataSnapshot snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name()).isEqualTo("segments_3");

        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name()).isEqualTo("segments_3");

        assertThat(recoverFromStore(newShard)).isEqualTo(true);

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name()).isEqualTo("segments_3");

        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name()).isEqualTo("segments_3");

        newShard.close("test", false);

        snapshot = newShard.snapshotStoreMetadata();
        assertThat(snapshot.getSegmentsFile().name()).isEqualTo("segments_3");

        closeShards(newShard);
    }

    @Test
    public void testAsyncFsync() throws InterruptedException, IOException {
        IndexShard shard = newStartedShard();
        Semaphore semaphore = new Semaphore(Integer.MAX_VALUE);
        Thread[] thread = new Thread[randomIntBetween(3, 5)];
        CountDownLatch latch = new CountDownLatch(thread.length);
        for (int i = 0; i < thread.length; i++) {
            thread[i] = new Thread(() -> {
                try {
                    latch.countDown();
                    latch.await();
                    for (int i1 = 0; i1 < 10000; i1++) {
                        semaphore.acquire();
                        shard.sync(TranslogTests.randomTranslogLocation(), (ex) -> semaphore.release());
                    }
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
            thread[i].start();
        }

        for (int i = 0; i < thread.length; i++) {
            thread[i].join();
        }
        assertThat(semaphore.tryAcquire(Integer.MAX_VALUE, 10, TimeUnit.SECONDS)).isEqualTo(true);

        closeShards(shard);
    }

    @Test
    public void testMinimumCompatVersion() throws IOException {
        Version versionCreated = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, versionCreated.internalId)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("test")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard test = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoverShardFromStore(test);

        indexDoc(test, "test");
        assertThat(versionCreated.luceneVersion).isEqualTo(test.minimumCompatibleVersion());
        closeShards(test);
    }

    @Test
    public void testShardStatsWithFailures() throws IOException {
        allowShardFailures();
        ShardId shardId = new ShardId("index", "_na_", 0);
        ShardRouting shardRouting = newShardRouting(
            shardId,
            "node",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(createTempDir());

        ShardPath shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder(shardRouting.getIndexName())
            .settings(settings)
            .primaryTerm(0, 1)
            .build();

        // Override two Directory methods to make them fail at our will
        // We use AtomicReference here to inject failure in the middle of the test not immediately
        // We use Supplier<IOException> instead of IOException to produce meaningful stacktrace
        // (remember stack trace is filled when exception is instantiated)
        AtomicReference<Supplier<IOException>> exceptionToThrow = new AtomicReference<>();
        AtomicBoolean throwWhenMarkingStoreCorrupted = new AtomicBoolean(false);
        Directory directory = new FilterDirectory(newFSDirectory(shardPath.resolveIndex())) {
            //fileLength method is called during storeStats try block
            //it's not called when store is marked as corrupted
            @Override
            public long fileLength(String name) throws IOException {
                Supplier<IOException> ex = exceptionToThrow.get();
                if (ex == null) {
                    return super.fileLength(name);
                } else {
                    throw ex.get();
                }
            }

            //listAll method is called when marking store as corrupted
            @Override
            public String[] listAll() throws IOException {
                Supplier<IOException> ex = exceptionToThrow.get();
                if (throwWhenMarkingStoreCorrupted.get() && ex != null) {
                    throw ex.get();
                } else {
                    return super.listAll();
                }
            }
        };

        try (Store store = createStore(shardId, new IndexSettings(metaData, Settings.EMPTY), directory)) {
            IndexShard shard = newShard(
                shardRouting,
                shardPath,
                metaData,
                i -> store,
                List.of(),
                () -> { },
                RetentionLeaseSyncer.EMPTY,
                EMPTY_EVENT_LISTENER);
            AtomicBoolean failureCallbackTriggered = new AtomicBoolean(false);
            shard.addShardFailureCallback((ig) -> failureCallbackTriggered.set(true));

            recoverShardFromStore(shard);

            boolean corruptIndexException = randomBoolean();
            if (corruptIndexException) {
                exceptionToThrow.set(() -> new CorruptIndexException("Test CorruptIndexException", "Test resource"));
                throwWhenMarkingStoreCorrupted.set(randomBoolean());
            } else {
                exceptionToThrow.set(() -> new IOException("Test IOException"));
            }
            assertThatThrownBy(shard::storeStats)
                .isExactlyInstanceOf(ElasticsearchException.class);
            assertThat(failureCallbackTriggered.get()).isEqualTo(true);

            if (corruptIndexException && !throwWhenMarkingStoreCorrupted.get()) {
                assertThat(store.isMarkedCorrupted()).isTrue();
            }
        }
    }

    @Test
    public void testIndexingOperationsListeners() throws IOException {
        IndexShard shard = newStartedShard(true);
        updateMappings(shard, IndexMetadata.builder(shard.indexSettings.getIndexMetadata())
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}").build());
        indexDoc(shard, "0", "{\"foo\" : \"bar\"}");
        shard.updateLocalCheckpointForShard(shard.shardRouting.allocationId().getId(), 0);
        AtomicInteger preIndex = new AtomicInteger();
        AtomicInteger postIndexCreate = new AtomicInteger();
        AtomicInteger postIndexUpdate = new AtomicInteger();
        AtomicInteger postIndexException = new AtomicInteger();
        AtomicInteger preDelete = new AtomicInteger();
        AtomicInteger postDelete = new AtomicInteger();
        AtomicInteger postDeleteException = new AtomicInteger();
        shard.close("simon says", true);
        shard = reinitShard(shard, new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
                switch (result.getResultType()) {
                    case SUCCESS:
                        if (result.isCreated()) {
                            postIndexCreate.incrementAndGet();
                        } else {
                            postIndexUpdate.incrementAndGet();
                        }
                        break;
                    case FAILURE:
                        postIndex(shardId, index, result.getFailure());
                        break;
                    default:
                        fail("unexpected result type:" + result.getResultType());
                }
            }

            @Override
            public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
                postIndexException.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
                switch (result.getResultType()) {
                    case SUCCESS:
                        postDelete.incrementAndGet();
                        break;
                    case FAILURE:
                        postDelete(shardId, delete, result.getFailure());
                        break;
                    default:
                        fail("unexpected result type:" + result.getResultType());
                }
            }

            @Override
            public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
                postDeleteException.incrementAndGet();

            }
        });
        recoverShardFromStore(shard);

        indexDoc(shard, "1");
        assertThat(1).isEqualTo(preIndex.get());
        assertThat(1).isEqualTo(postIndexCreate.get());
        assertThat(0).isEqualTo(postIndexUpdate.get());
        assertThat(0).isEqualTo(postIndexException.get());
        assertThat(0).isEqualTo(preDelete.get());
        assertThat(0).isEqualTo(postDelete.get());
        assertThat(0).isEqualTo(postDeleteException.get());

        indexDoc(shard, "1");
        assertThat(preIndex.get()).isEqualTo(2);
        assertThat(postIndexCreate.get()).isEqualTo(1);
        assertThat(postIndexUpdate.get()).isEqualTo(1);
        assertThat(postIndexException.get()).isEqualTo(0);
        assertThat(preDelete.get()).isEqualTo(0);
        assertThat(postDelete.get()).isEqualTo(0);
        assertThat(postDeleteException.get()).isEqualTo(0);

        deleteDoc(shard, "1");

        assertThat(preIndex.get()).isEqualTo(2);
        assertThat(postIndexCreate.get()).isEqualTo(1);
        assertThat(postIndexUpdate.get()).isEqualTo(1);
        assertThat(postIndexException.get()).isEqualTo(0);
        assertThat(preDelete.get()).isEqualTo(1);
        assertThat(postDelete.get()).isEqualTo(1);
        assertThat(postDeleteException.get()).isEqualTo(0);

        shard.close("Unexpected close", true);
        shard.state = IndexShardState.STARTED; // It will generate exception

        try {
            indexDoc(shard, "1");
            fail();
        } catch (AlreadyClosedException ignored) {

        }

        assertThat(preIndex.get()).isEqualTo(2);
        assertThat(postIndexCreate.get()).isEqualTo(1);
        assertThat(postIndexUpdate.get()).isEqualTo(1);
        assertThat(postIndexException.get()).isEqualTo(0);
        assertThat(preDelete.get()).isEqualTo(1);
        assertThat(postDelete.get()).isEqualTo(1);
        assertThat(postDeleteException.get()).isEqualTo(0);

        try {
            deleteDoc(shard, "1");
            fail();
        } catch (AlreadyClosedException ignored) {

        }
        assertThat(preIndex.get()).isEqualTo(2);
        assertThat(postIndexCreate.get()).isEqualTo(1);
        assertThat(postIndexUpdate.get()).isEqualTo(1);
        assertThat(postIndexException.get()).isEqualTo(0);
        assertThat(preDelete.get()).isEqualTo(1);
        assertThat(postDelete.get()).isEqualTo(1);
        assertThat(postDeleteException.get()).isEqualTo(0);

        closeShards(shard);
    }

    @Test
    public void testLockingBeforeAndAfterRelocated() throws Exception {
        IndexShard shard = newStartedShard(true);
        ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        CountDownLatch latch = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            latch.countDown();
            try {
                shard.relocated(
                    routing.getTargetRelocatingShard().allocationId().getId(),
                    primaryContext -> {});
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        try (Releasable ignored = acquirePrimaryOperationPermitBlockingly(shard)) {
            // start finalization of recovery
            recoveryThread.start();
            latch.await();
            // recovery can only be finalized after we release the current primaryOperationLock
            assertThat(shard.isRelocatedPrimary()).isEqualTo(false);
        }
        // recovery can be now finalized
        recoveryThread.join();
        assertThat(shard.isRelocatedPrimary()).isEqualTo(true);
        assertThatThrownBy(() -> acquirePrimaryOperationPermitBlockingly(shard))
            .isExactlyInstanceOf(ExecutionException.class)
            .cause()
                .isExactlyInstanceOf(ShardNotInPrimaryModeException.class)
                .hasMessageContaining("shard is not in primary mode");

        closeShards(shard);
    }

    @Test
    public void testDelayedOperationsBeforeAndAfterRelocated() throws Exception {
        IndexShard shard = newStartedShard(true);
        ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        CountDownLatch startRecovery = new CountDownLatch(1);
        CountDownLatch relocationStarted = new CountDownLatch(1);
        Thread recoveryThread = new Thread(() -> {
            try {
                startRecovery.await();
                shard.relocated(
                    routing.getTargetRelocatingShard().allocationId().getId(),
                    primaryContext -> relocationStarted.countDown());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        recoveryThread.start();

        int numberOfAcquisitions = randomIntBetween(1, 10);
        List<Runnable> assertions = new ArrayList<>(numberOfAcquisitions);
        int recoveryIndex = randomIntBetween(0, numberOfAcquisitions - 1);

        for (int i = 0; i < numberOfAcquisitions; i++) {
            PlainActionFuture<Releasable> onLockAcquired;
            if (i < recoveryIndex) {
                AtomicBoolean invoked = new AtomicBoolean();
                onLockAcquired = new PlainActionFuture<>() {

                    @Override
                    public void onResponse(Releasable releasable) {
                        invoked.set(true);
                        releasable.close();
                        super.onResponse(releasable);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        throw new AssertionError();
                    }

                };
                assertions.add(() -> assertThat(invoked.get()).isTrue());
            } else if (recoveryIndex == i) {
                startRecovery.countDown();
                relocationStarted.await();
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    assertThatThrownBy(() -> onLockAcquired.get(30, TimeUnit.SECONDS))
                        .isExactlyInstanceOf(ExecutionException.class)
                        .cause()
                            .isExactlyInstanceOf(ShardNotInPrimaryModeException.class)
                            .hasMessageContaining("shard is not in primary mode");
                });
            } else {
                onLockAcquired = new PlainActionFuture<>();
                assertions.add(() -> {
                    assertThatThrownBy(() -> onLockAcquired.get(30, TimeUnit.SECONDS))
                        .isExactlyInstanceOf(ExecutionException.class)
                        .cause()
                            .isExactlyInstanceOf(ShardNotInPrimaryModeException.class)
                            .hasMessageContaining("shard is not in primary mode");
                });
            }
            shard.acquirePrimaryOperationPermit(onLockAcquired, ThreadPool.Names.WRITE, "i_" + i);
        }
        for (Runnable assertion : assertions) {
            assertion.run();
        }
        recoveryThread.join();

        closeShards(shard);
    }

    @Test
    public void testStressRelocated() throws Exception {
        IndexShard shard = newStartedShard(true);
        assertThat(shard.isRelocatedPrimary()).isEqualTo(false);
        ShardRouting routing = ShardRoutingHelper.relocate(shard.routingEntry(), "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        int numThreads = randomIntBetween(2, 4);
        Thread[] indexThreads = new Thread[numThreads];
        CountDownLatch allPrimaryOperationLocksAcquired = new CountDownLatch(numThreads);
        CyclicBarrier barrier = new CyclicBarrier(numThreads + 1);
        for (int i = 0; i < indexThreads.length; i++) {
            indexThreads[i] = new Thread(() -> {
                try (Releasable operationLock = acquirePrimaryOperationPermitBlockingly(shard)) {
                    allPrimaryOperationLocksAcquired.countDown();
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            indexThreads[i].start();
        }
        AtomicBoolean relocated = new AtomicBoolean();
        final Thread recoveryThread = new Thread(() -> {
            try {
                shard.relocated(routing.getTargetRelocatingShard().allocationId().getId(), primaryContext -> { });
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            relocated.set(true);
        });
        // ensure we wait for all primary operation locks to be acquired
        allPrimaryOperationLocksAcquired.await();
        // start recovery thread
        recoveryThread.start();
        assertThat(relocated.get()).isEqualTo(false);
        assertThat(shard.getActiveOperationsCount()).isGreaterThan(0);
        // ensure we only transition after pending operations completed
        assertThat(shard.isRelocatedPrimary()).isEqualTo(false);
        // complete pending operations
        barrier.await();
        // complete recovery/relocation
        recoveryThread.join();
        // ensure relocated successfully once pending operations are done
        assertThat(relocated.get()).isEqualTo(true);
        assertThat(shard.isRelocatedPrimary()).isEqualTo(true);
        assertThat(shard.getActiveOperationsCount()).isEqualTo(0);

        for (Thread indexThread : indexThreads) {
            indexThread.join();
        }

        closeShards(shard);
    }

    @Test
    public void testRelocatedShardCanNotBeRevived() throws IOException, InterruptedException {
        IndexShard shard = newStartedShard(true);
        ShardRouting originalRouting = shard.routingEntry();
        ShardRouting routing = ShardRoutingHelper.relocate(originalRouting, "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, routing);
        shard.relocated(routing.getTargetRelocatingShard().allocationId().getId(), primaryContext -> { });
        assertThatThrownBy(() -> IndexShardTestCase.updateRoutingEntry(shard, originalRouting))
            .isInstanceOf(IllegalIndexShardStateException.class);
        closeShards(shard);
    }

    @Test
    public void testShardCanNotBeMarkedAsRelocatedIfRelocationCancelled() throws IOException {
        IndexShard shard = newStartedShard(true);
        ShardRouting originalRouting = shard.routingEntry();
        ShardRouting relocationRouting = ShardRoutingHelper.relocate(originalRouting, "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, relocationRouting);
        IndexShardTestCase.updateRoutingEntry(shard, originalRouting);
        assertThatThrownBy(() -> shard.relocated(
                relocationRouting.getTargetRelocatingShard().allocationId().getId(),
                primaryContext -> {
                })
        ).isExactlyInstanceOf(IllegalIndexShardStateException.class);
        closeShards(shard);
    }

    @Test
    public void testRelocatedShardCanNotBeRevivedConcurrently() throws IOException, InterruptedException, BrokenBarrierException {
        IndexShard shard = newStartedShard(true);
        ShardRouting originalRouting = shard.routingEntry();
        ShardRouting relocationRouting = ShardRoutingHelper.relocate(originalRouting, "other_node");
        IndexShardTestCase.updateRoutingEntry(shard, relocationRouting);
        CyclicBarrier cyclicBarrier = new CyclicBarrier(3);
        AtomicReference<Exception> relocationException = new AtomicReference<>();
        Thread relocationThread = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                relocationException.set(e);
            }

            @Override
            protected void doRun() throws Exception {
                cyclicBarrier.await();
                shard.relocated(
                    relocationRouting.getTargetRelocatingShard().allocationId().getId(),
                    primaryContext -> { });
            }
        });
        relocationThread.start();
        AtomicReference<Exception> cancellingException = new AtomicReference<>();
        Thread cancellingThread = new Thread(new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                cancellingException.set(e);
            }

            @Override
            protected void doRun() throws Exception {
                cyclicBarrier.await();
                IndexShardTestCase.updateRoutingEntry(shard, originalRouting);
            }
        });
        cancellingThread.start();
        cyclicBarrier.await();
        relocationThread.join();
        cancellingThread.join();
        if (shard.isRelocatedPrimary()) {
            logger.debug("shard was relocated successfully");
            assertThat(cancellingException.get()).isInstanceOf(IllegalIndexShardStateException.class);
            assertThat(shard.routingEntry().relocating())
                .as("current routing:" + shard.routingEntry())
                .isEqualTo(true);
            assertThat(relocationException.get()).isNull();
        } else {
            logger.debug("shard relocation was cancelled");
            assertThat(relocationException.get()).satisfiesAnyOf(
                x -> assertThat(x).isInstanceOf(IllegalIndexShardStateException.class),
                x -> assertThat(x).isExactlyInstanceOf(IllegalStateException.class)
            );
            assertThat(shard.routingEntry().relocating())
                .as("current routing:" + shard.routingEntry())
                .isEqualTo(false);
            assertThat(cancellingException.get()).isNull();

        }
        closeShards(shard);
    }

    /* This test just verifies that we fill up local checkpoint up
     to max seen seqID on primary recovery */
    @Test
    public void testRecoverFromStoreWithNoOps() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean()).build();
        IndexShard shard = newStartedShard(true, settings);
        indexDoc(shard, "0");
        indexDoc(shard, "1");
        // start a replica shard and index the second doc
        IndexShard otherShard = newStartedShard(false, settings);
        updateMappings(otherShard, shard.indexSettings().getIndexMetadata());
        SourceToParse sourceToParse = new SourceToParse(
            shard.shardId().getIndexName(), "1", new BytesArray("{}"), XContentType.JSON);
        otherShard.applyIndexOperationOnReplica(
            1, primaryTerm, 1, UNSET_AUTO_GENERATED_TIMESTAMP, false, sourceToParse);
        ShardRouting primaryShardRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(
            otherShard,
            ShardRoutingHelper.initWithSameId(
                primaryShardRouting,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE)
        );
        DiscoveryNode localNode = new DiscoveryNode(
            "foo", buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertThat(recoverFromStore(newShard)).isEqualTo(true);
        assertThat(newShard.recoveryState().getTranslog().recoveredOperations()).isEqualTo(1);
        assertThat(newShard.recoveryState().getTranslog().totalOperations()).isEqualTo(1);
        assertThat(newShard.recoveryState().getTranslog().totalOperationsOnStart()).isEqualTo(1);
        assertThat(newShard.recoveryState().getTranslog().recoveredPercent()).isEqualTo(100.0f, Offset.offset(0.01f));
        try (Translog.Snapshot snapshot = getTranslog(newShard).newSnapshot()) {
            Translog.Operation operation;
            int numNoops = 0;
            while ((operation = snapshot.next()) != null) {
                if (operation.opType() == Translog.Operation.Type.NO_OP) {
                    numNoops++;
                    assertThat(newShard.getPendingPrimaryTerm()).isEqualTo(operation.primaryTerm());
                    assertThat(0).isEqualTo(operation.seqNo());
                }
            }
            assertThat(1).isEqualTo(numNoops);
        }
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 1);
        assertDocCount(shard, 2);

        for (int i = 0; i < 2; i++) {
            newShard = reinitShard(newShard, ShardRoutingHelper.initWithSameId(
                primaryShardRouting, RecoverySource.ExistingStoreRecoverySource.INSTANCE));
            newShard.markAsRecovering(
                "store",
                new RecoveryState(newShard.routingEntry(), localNode, null));
            assertThat(recoverFromStore(newShard)).isEqualTo(true);
            try (Translog.Snapshot snapshot = getTranslog(newShard).newSnapshot()) {
                assertThat(snapshot.totalOperations()).isEqualTo(newShard.indexSettings.isSoftDeleteEnabled() ? 0 : 2);
            }
        }
        closeShards(newShard, shard);
    }

    @Test
    public void testRecoverFromCleanStore() throws IOException {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "0");
        if (randomBoolean()) {
            flushShard(shard);
        }
        ShardRouting shardRouting = shard.routingEntry();
        IndexShard newShard = reinitShard(
            shard,
            ShardRoutingHelper.initWithSameId(shardRouting, RecoverySource.EmptyStoreRecoverySource.INSTANCE)
        );

        DiscoveryNode localNode = new DiscoveryNode(
            "foo", buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertThat(recoverFromStore(newShard)).isEqualTo(true);
        assertThat(newShard.recoveryState().getTranslog().recoveredOperations()).isEqualTo(0);
        assertThat(newShard.recoveryState().getTranslog().totalOperations()).isEqualTo(0);
        assertThat(newShard.recoveryState().getTranslog().totalOperationsOnStart()).isEqualTo(0);
        assertThat(newShard.recoveryState().getTranslog().recoveredPercent()).isEqualTo(100.0f, Offset.offset(0.01f));
        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 0);
        closeShards(newShard);
    }

    @Test
    public void testFailIfIndexNotPresentInRecoverFromStore() throws Exception {
        IndexShard shard = newStartedShard(true);
        indexDoc(shard, "0");
        if (randomBoolean()) {
            flushShard(shard);
        }

        Store store = shard.store();
        store.incRef();
        closeShards(shard);
        cleanLuceneIndex(store.directory());
        store.decRef();
        IndexShard newShard = reinitShard(shard);
        DiscoveryNode localNode = new DiscoveryNode(
            "foo", buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
        ShardRouting routing = newShard.routingEntry();
        newShard.markAsRecovering("store", new RecoveryState(routing, localNode, null));
        try {
            recoverFromStore(newShard);
            fail("index not there!");
        } catch (IndexShardRecoveryException ex) {
            assertThat(ex.getMessage().contains("failed to fetch index version after copying it over")).isTrue();
        }

        routing = ShardRoutingHelper.moveToUnassigned(
            routing,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "because I say so"));
        routing = ShardRoutingHelper.initialize(routing, newShard.routingEntry().currentNodeId());
        assertThat(newShard.ignoreRecoveryAttempt())
            .as("it's already recovering, we should ignore new ones")
            .isTrue();
        try {
            newShard.markAsRecovering("store", new RecoveryState(routing, localNode, null));
            fail("we are already recovering, can't mark again");
        } catch (IllegalIndexShardStateException e) {
            // OK!
        }

        newShard = reinitShard(
            newShard,
            ShardRoutingHelper.initWithSameId(
                routing,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE)
        );
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertThat(recoverFromStore(newShard))
            .as("recover even if there is nothing to recover")
            .isTrue();

        IndexShardTestCase.updateRoutingEntry(newShard, newShard.routingEntry().moveToStarted());
        assertDocCount(newShard, 0);
        // we can't issue this request through a client because of the
        // inconsistencies we created with the cluster state doing it directly instead
        indexDoc(newShard, "0");
        newShard.refresh("test");
        assertDocCount(newShard, 1);

        closeShards(newShard);
    }

    @Test
    public void testRecoverFromStoreRemoveStaleOperations() throws Exception {
        IndexShard shard = newStartedShard(false);
        String indexName = shard.shardId().getIndexName();
        // Index #0, index #1
        shard.applyIndexOperationOnReplica(
            0,
            primaryTerm,
            1,
            UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse(indexName, "doc-0", new BytesArray("{}"), XContentType.JSON));
        flushShard(shard);
        shard.updateGlobalCheckpointOnReplica(0, "test"); // stick the global checkpoint here.
        shard.applyIndexOperationOnReplica(
            1,
            primaryTerm,
            1,
            UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse(indexName, "doc-1", new BytesArray("{}"), XContentType.JSON));
        flushShard(shard);
        assertThat(getShardDocUIDs(shard)).containsExactlyInAnyOrder("doc-0", "doc-1");
        shard.getEngine().rollTranslogGeneration();
        shard.markSeqNoAsNoop(
            1,
            shard.getOperationPrimaryTerm(),
            "test"
        );
        shard.applyIndexOperationOnReplica(
            2,
            primaryTerm,
            1,
            UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            new SourceToParse(indexName, "doc-2", new BytesArray("{}"), XContentType.JSON));
        flushShard(shard);
        assertThat(getShardDocUIDs(shard)).containsExactlyInAnyOrder("doc-0", "doc-1", "doc-2");
        closeShard(shard, false);
        // Recovering from store should discard doc #1
        ShardRouting replicaRouting = shard.routingEntry();
        IndexMetadata newShardIndexMetadata = IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
            .primaryTerm(replicaRouting.shardId().id(), shard.getOperationPrimaryTerm() + 1)
            .build();
        closeShards(shard);
        IndexShard newShard = newShard(
            newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.ExistingStoreRecoverySource.INSTANCE),
            shard.shardPath(),
            newShardIndexMetadata,
            null,
            shard.engineFactoryProviders(),
            shard.getGlobalCheckpointSyncer(),
            shard.getRetentionLeaseSyncer(),
            EMPTY_EVENT_LISTENER);
        DiscoveryNode localNode = new DiscoveryNode(
            "foo", buildNewFakeTransportAddress(), Map.of(), Set.of(), Version.CURRENT);
        newShard.markAsRecovering("store", new RecoveryState(newShard.routingEntry(), localNode, null));
        assertThat(recoverFromStore(newShard)).isTrue();
        assertThat(getShardDocUIDs(newShard)).containsExactlyInAnyOrder("doc-0", "doc-2");
        closeShards(newShard);
    }

    @Test
    public void testRestoreShard() throws Exception {
        IndexShard source = newStartedShard(true);
        IndexShard target = newStartedShard(
            true,
            Settings.builder()
                .put(
                    IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(),
                    source.indexSettings().isSoftDeleteEnabled())
                .build());

        indexDoc(source, "0");
        EngineTestCase.generateNewSeqNo(source.getEngine()); // create a gap in the history
        indexDoc(source, "2");
        if (randomBoolean()) {
            source.refresh("test");
        }
        indexDoc(target, "1");
        target.refresh("test");
        assertThat(getShardDocUIDs(target)).containsExactly("1");
        flushShard(source); // only flush source
        ShardRouting routing = ShardRoutingHelper.initWithSameId(
            target.routingEntry(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE);
        Snapshot snapshot = new Snapshot("foo", new SnapshotId("bar", UUIDs.randomBase64UUID()));
        routing = ShardRoutingHelper.newWithRestoreSource(
            routing,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                snapshot,
                Version.CURRENT,
                new IndexId("test", UUIDs.randomBase64UUID(random())))
        );
        target = reinitShard(target, routing);
        Store sourceStore = source.store();
        Store targetStore = target.store();

        DiscoveryNode localNode = new DiscoveryNode(
            "foo",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT);
        target.markAsRecovering("store", new RecoveryState(routing, localNode, null));
        final FutureActionListener<Boolean> future = new FutureActionListener<>();
        target.restoreFromRepository(new RestoreOnlyRepository("test") {

                @Override
                public void restoreShard(Store store,
                                         SnapshotId snapshotId,
                                         IndexId indexId,
                                         ShardId snapshotShardId,
                                         RecoveryState recoveryState,
                                         ActionListener<Void> listener) {
                    ActionListener.completeWith(listener, () -> {
                        cleanLuceneIndex(targetStore.directory());
                        for (String file : sourceStore.directory().listAll()) {
                            if (file.equals("write.lock") || file.startsWith("extra")) {
                                continue;
                            }
                            targetStore
                                .directory()
                                .copyFrom(sourceStore.directory(), file, file, IOContext.DEFAULT);
                        }
                        recoveryState.getIndex().setFileDetailsComplete();
                        return null;
                    });
                }

            @Override
            public void updateState(ClusterState state) {}

            @Override
            public void executeConsistentStateUpdate(Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
                                                     String source,
                                                     Consumer<Exception> onFailure) {}
        }, future);
        assertThat(future.get(5, TimeUnit.SECONDS)).isEqualTo(true);
        assertThat(target.getLocalCheckpoint()).isEqualTo(2L);
        assertThat(target.seqNoStats().getMaxSeqNo()).isEqualTo(2L);
        assertThat(target.seqNoStats().getGlobalCheckpoint()).isEqualTo(0L);
        IndexShardTestCase.updateRoutingEntry(target, routing.moveToStarted());
        assertThat(
            target
                .getReplicationTracker()
                .getTrackedLocalCheckpointForShard(
                    target.routingEntry().allocationId().getId()
                ).getLocalCheckpoint())
            .isEqualTo(2L);
        assertThat(target.seqNoStats().getGlobalCheckpoint()).isEqualTo(2L);
        assertThat(getShardDocUIDs(target)).containsExactly("0", "2");

        closeShard(source, false);
        closeShards(target);
    }

    @Test
    public void testFlushOnInactive() throws Exception {
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("test")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        ShardRouting shardRouting =
            TestShardRouting.newShardRouting(
                new ShardId(metaData.getIndex(), 0),
                "n1",
                true,
                ShardRoutingState.INITIALIZING,
                RecoverySource.EmptyStoreRecoverySource.INSTANCE);
        final ShardId shardId = shardRouting.shardId();
        final NodeEnvironment.NodePath nodePath = new NodeEnvironment.NodePath(createTempDir());
        ShardPath shardPath = new ShardPath(false, nodePath.resolve(shardId), nodePath.resolve(shardId), shardId);
        AtomicBoolean markedInactive = new AtomicBoolean();
        AtomicReference<IndexShard> primaryRef = new AtomicReference<>();
        IndexShard primary = newShard(
            shardRouting,
            shardPath,
            metaData,
            null,
            List.of(),
            () -> { },
            new IndexEventListener() {
                @Override
                public void onShardInactive(IndexShard indexShard) {
                    markedInactive.set(true);
                    primaryRef.get().flush(new FlushRequest());
                }
            });
        primaryRef.set(primary);
        recoverShardFromStore(primary);
        for (int i = 0; i < 3; i++) {
            indexDoc(primary, String.valueOf(i), "{\"foo\" : \"" + randomAlphaOfLength(10) + "\"}");
            primary.refresh("test"); // produce segments
        }
        List<Segment> segments = primary.segments(false);
        Set<String> names = new HashSet<>();
        for (Segment segment : segments) {
            assertThat(segment.committed).isFalse();
            assertThat(segment.search).isTrue();
            names.add(segment.getName());
        }
        assertThat(segments.size()).isEqualTo(3);
        primary.flush(new FlushRequest());
        primary.forceMerge(new ForceMergeRequest().maxNumSegments(1).flush(false));
        primary.refresh("test");
        segments = primary.segments(false);
        for (Segment segment : segments) {
            if (names.contains(segment.getName())) {
                assertThat(segment.committed).isTrue();
                assertThat(segment.search).isFalse();
            } else {
                assertThat(segment.committed).isFalse();
                assertThat(segment.search).isTrue();
            }
        }
        assertThat(segments.size()).isEqualTo(4);

        assertThat(markedInactive.get()).isFalse();
        assertBusy(() -> {
            primary.checkIdle(0);
            assertThat(primary.isActive()).isFalse();
        });

        assertThat(markedInactive.get()).isTrue();
        segments = primary.segments(false);
        assertThat(1).isEqualTo(segments.size());
        for (Segment segment : segments) {
            assertThat(segment.committed).isTrue();
            assertThat(segment.search).isTrue();
        }
        closeShards(primary);
    }

    @Test
    public void testOnCloseStats() throws IOException {
        IndexShard indexShard = newStartedShard(true);
        updateMappings(indexShard, IndexMetadata.builder(indexShard.indexSettings.getIndexMetadata())
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}").build());
        for (int i = 0; i < 3; i++) {
            indexDoc(indexShard, String.valueOf(i), "{\"foo\" : \"" + randomAlphaOfLength(10) + "\"}");
            indexShard.refresh("test"); // produce segments
        }

        // check stats on closed and on opened shard
        if (randomBoolean()) {
            closeShards(indexShard);

            assertThatThrownBy(indexShard::seqNoStats).isExactlyInstanceOf(AlreadyClosedException.class);
            assertThatThrownBy(indexShard::commitStats).isExactlyInstanceOf(AlreadyClosedException.class);
            assertThatThrownBy(indexShard::storeStats).isExactlyInstanceOf(AlreadyClosedException.class);
        } else {
            SeqNoStats seqNoStats = indexShard.seqNoStats();
            assertThat(seqNoStats.getLocalCheckpoint()).isEqualTo(2L);

            CommitStats commitStats = indexShard.commitStats();
            assertThat(commitStats.getGeneration()).isEqualTo(2L);

            StoreStats storeStats = indexShard.storeStats();
            assertThat(storeStats.sizeInBytes()).isGreaterThan(0L);

            closeShards(indexShard);
        }
    }

    @Test
    public void testSupplyTombstoneDoc() throws Exception {
        IndexShard shard = newStartedShard();
        String id = randomRealisticUnicodeOfLengthBetween(1, 10);
        ParsedDocument deleteTombstone = shard.getEngine().config().getTombstoneDocSupplier().newDeleteTombstoneDoc(id);
        Document deleteDoc = deleteTombstone.doc();
        assertThat(deleteDoc.getFields().stream().map(IndexableField::name).toList())
            .containsExactlyInAnyOrder(
                IdFieldMapper.NAME,
                DocSysColumns.VERSION.name(),
                DocSysColumns.Names.SEQ_NO,
                DocSysColumns.Names.SEQ_NO,
                DocSysColumns.Names.PRIMARY_TERM,
                DocSysColumns.Names.TOMBSTONE);
        assertThat(deleteDoc.getField(IdFieldMapper.NAME).binaryValue()).isEqualTo(Uid.encodeId(id));
        assertThat(deleteDoc.getField(DocSysColumns.Names.TOMBSTONE).numericValue().longValue()).isEqualTo(1L);

        updateMappings(shard, IndexMetadata.builder(shard.indexSettings.getIndexMetadata())
            .putMapping("{ \"properties\": {}}").build());
        final String reason = randomUnicodeOfLength(200);
        ParsedDocument noopTombstone = shard.getEngine().config().getTombstoneDocSupplier().newNoopTombstoneDoc(reason);
        Document noopDoc = noopTombstone.doc();
        assertThat(noopDoc.getFields().stream().map(IndexableField::name).toList())
            .containsExactlyInAnyOrder(
                DocSysColumns.VERSION.name(),
                SourceFieldMapper.NAME,
                DocSysColumns.Names.TOMBSTONE,
                DocSysColumns.Names.SEQ_NO,
                DocSysColumns.Names.SEQ_NO,
                DocSysColumns.Names.PRIMARY_TERM);
        assertThat(noopDoc.getField(DocSysColumns.Names.TOMBSTONE).numericValue().longValue()).isEqualTo(1L);
        assertThat(noopDoc.getField(SourceFieldMapper.NAME).binaryValue()).isEqualTo(new BytesRef(reason));

        closeShards(shard);
    }

    @Test
    public void testResetEngine() throws Exception {
        IndexShard shard = newStartedShard(false);
        indexOnReplicaWithGaps(shard, between(0, 1000), Math.toIntExact(shard.getLocalCheckpoint()), false);
        long maxSeqNoBeforeRollback = shard.seqNoStats().getMaxSeqNo();
        final long globalCheckpoint = randomLongBetween(shard.getLastKnownGlobalCheckpoint(), shard.getLocalCheckpoint());
        shard.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");
        Set<String> docBelowGlobalCheckpoint = getShardDocUIDs(shard).stream()
            .filter(id -> Long.parseLong(id) <= globalCheckpoint).collect(Collectors.toSet());
        TranslogStats translogStats = shard.translogStats();
        AtomicBoolean done = new AtomicBoolean();
        CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(() -> {
            latch.countDown();
            int hitClosedExceptions = 0;
            while (done.get() == false) {
                try {
                    List<String> exposedDocIds = EngineTestCase.getDocIds(getEngine(shard), rarely())
                        .stream().map(DocIdSeqNoAndSource::getId).collect(Collectors.toList());
                    assertThat(docBelowGlobalCheckpoint)
                        .as("every operations before the global checkpoint must be reserved")
                        .isSubsetOf(exposedDocIds);
                } catch (AlreadyClosedException ignored) {
                    hitClosedExceptions++;
                } catch (IOException e) {
                    throw new AssertionError(e);
                }
            }
            // engine reference was switched twice: current read/write engine -> ready-only engine -> new read/write engine
            assertThat(hitClosedExceptions).isLessThanOrEqualTo(2);
        });
        thread.start();
        latch.await();

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(shard.getOperationPrimaryTerm(), globalCheckpoint, 0L, ActionListener.wrap(r -> {
            try {
                shard.resetEngineToGlobalCheckpoint();
            } finally {
                r.close();
                engineResetLatch.countDown();
            }
        }, Assert::assertNotNull), TimeValue.timeValueMinutes(1L));
        engineResetLatch.await();
        assertThat(getShardDocUIDs(shard)).isEqualTo(docBelowGlobalCheckpoint);
        assertThat(shard.seqNoStats().getMaxSeqNo()).isEqualTo(globalCheckpoint);
        if (shard.indexSettings.isSoftDeleteEnabled()) {
            // we might have trimmed some operations if the translog retention policy is ignored (when soft-deletes enabled).
            assertThat(shard.translogStats().estimatedNumberOfOperations())
                .isLessThanOrEqualTo(translogStats.estimatedNumberOfOperations());
        } else {
            assertThat(shard.translogStats().estimatedNumberOfOperations()).isEqualTo(translogStats.estimatedNumberOfOperations());
        }
        assertThat(shard.getMaxSeqNoOfUpdatesOrDeletes()).isEqualTo(maxSeqNoBeforeRollback);
        done.set(true);
        thread.join();
        closeShard(shard, false);
    }

    /**
     * This test simulates a scenario seen rarely in ConcurrentSeqNoVersioningIT. Closing a shard while engine is inside
     * resetEngineToGlobalCheckpoint can lead to check index failure in integration tests.
     */
    @Test
    public void testCloseShardWhileResettingEngine() throws Exception {
        CountDownLatch readyToCloseLatch = new CountDownLatch(1);
        CountDownLatch closeDoneLatch = new CountDownLatch(1);
        IndexShard shard = newStartedShard(false, Settings.EMPTY, List.of(idxSettings -> Optional.of(
            config -> new InternalEngine(config) {
                @Override
                public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner,
                                                          long recoverUpToSeqNo) throws IOException {
                    readyToCloseLatch.countDown();
                    try {
                        closeDoneLatch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    return super.recoverFromTranslog(translogRecoveryRunner, recoverUpToSeqNo);
                }
        })));

        Thread closeShardThread = new Thread(() -> {
            try {
                readyToCloseLatch.await();
                shard.close("testing", false);
                // in integration tests, this is done as a listener on IndexService.
                MockFSDirectoryFactory.checkIndex(logger, shard.store(), shard.shardId);
            } catch (InterruptedException | IOException e) {
                throw new AssertionError(e);
            } finally {
                closeDoneLatch.countDown();
            }
        });

        closeShardThread.start();

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(shard.getOperationPrimaryTerm(), shard.getLastKnownGlobalCheckpoint(), 0L,
            ActionListener.wrap(r -> {
                try (r) {
                    shard.resetEngineToGlobalCheckpoint();
                } finally {
                    engineResetLatch.countDown();
                }
            }, Assert::assertNotNull), TimeValue.timeValueMinutes(1L));

        engineResetLatch.await();

        closeShardThread.join();

        // close store.
        closeShard(shard, false);
    }

    @Test
    public void testResetEngineWithBrokenTranslog() throws Exception {
        IndexShard shard = newStartedShard(false);
        updateMappings(shard, IndexMetadata.builder(shard.indexSettings.getIndexMetadata())
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}").build());
        final List<Translog.Operation> operations = Stream.concat(
            IntStream.range(0, randomIntBetween(0, 10)).mapToObj(n -> new Translog.Index(
                "1",
                0,
                shard.getPendingPrimaryTerm(),
                1,
                "{\"foo\" : \"bar\"}".getBytes(StandardCharsets.UTF_8),
                -1)
            ),
            // entries with corrupted source
            IntStream.range(0, randomIntBetween(1, 10)).mapToObj(n -> new Translog.Index(
                "1",
                0,
                shard.getPendingPrimaryTerm(),
                1,
                "{\"foo\" : \"bar}".getBytes(StandardCharsets.UTF_8),
                -1)
            )).collect(Collectors.toList());
        Randomness.shuffle(operations);
        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(shard.getOperationPrimaryTerm(), shard.getLastKnownGlobalCheckpoint(), 0L,
            ActionListener.wrap(
                r -> {
                    try (r) {
                        Translog.Snapshot snapshot = TestTranslog.newSnapshotFromOperations(operations);
                        assertThatThrownBy(() -> shard.runTranslogRecovery(shard.getEngine(), snapshot, Engine.Operation.Origin.LOCAL_RESET, () -> {}))
                            .isExactlyInstanceOf(MapperParsingException.class)
                            .hasMessageContaining("failed to parse field [foo] of type [text]");
                    } finally {
                        engineResetLatch.countDown();
                    }
                },
                e -> {
                    throw new AssertionError(e);
                }), TimeValue.timeValueMinutes(1));
        engineResetLatch.await();
        closeShards(shard);
    }

    /**
     * This test simulates a scenario seen rarely in ConcurrentSeqNoVersioningIT. While engine is inside
     * resetEngineToGlobalCheckpoint snapshot metadata could fail
     */
    @Test
    public void testSnapshotWhileResettingEngine() throws Exception {
        CountDownLatch readyToSnapshotLatch = new CountDownLatch(1);
        CountDownLatch snapshotDoneLatch = new CountDownLatch(1);
        IndexShard shard = newStartedShard(false, Settings.EMPTY, List.of(idxSettings -> Optional.of(
            config -> new InternalEngine(config) {
                @Override
                public InternalEngine recoverFromTranslog(TranslogRecoveryRunner translogRecoveryRunner,
                                                          long recoverUpToSeqNo) throws IOException {
                    InternalEngine internalEngine = super.recoverFromTranslog(translogRecoveryRunner, recoverUpToSeqNo);
                    readyToSnapshotLatch.countDown();
                    try {
                        snapshotDoneLatch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    return internalEngine;
                }
        })));

        indexOnReplicaWithGaps(shard, between(0, 1000), Math.toIntExact(shard.getLocalCheckpoint()), true);
        final long globalCheckpoint = randomLongBetween(shard.getLastKnownGlobalCheckpoint(), shard.getLocalCheckpoint());
        shard.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");

        Thread snapshotThread = new Thread(() -> {
            try {
                readyToSnapshotLatch.await();
                shard.snapshotStoreMetadata();
                try (Engine.IndexCommitRef indexCommitRef = shard.acquireLastIndexCommit(randomBoolean())) {
                    shard.store().getMetadata(indexCommitRef.getIndexCommit());
                }
                try (Engine.IndexCommitRef indexCommitRef = shard.acquireSafeIndexCommit()) {
                    shard.store().getMetadata(indexCommitRef.getIndexCommit());
                }
            } catch (InterruptedException | IOException e) {
                throw new AssertionError(e);
            } finally {
                snapshotDoneLatch.countDown();
            }
        });

        snapshotThread.start();

        final CountDownLatch engineResetLatch = new CountDownLatch(1);
        shard.acquireAllReplicaOperationsPermits(shard.getOperationPrimaryTerm(), shard.getLastKnownGlobalCheckpoint(), 0L,
            ActionListener.wrap(r -> {
                try (r) {
                    shard.resetEngineToGlobalCheckpoint();
                } finally {
                    engineResetLatch.countDown();
                }
            }, Assert::assertNotNull), TimeValue.timeValueMinutes(1L));

        engineResetLatch.await();

        snapshotThread.join();

        closeShard(shard, false);
    }

    @Test
    public void testRelocateMissingTarget() throws Exception {
        final IndexShard shard = newStartedShard(true);
        final ShardRouting original = shard.routingEntry();
        final ShardRouting toNode1 = ShardRoutingHelper.relocate(original, "node_1");
        IndexShardTestCase.updateRoutingEntry(shard, toNode1);
        IndexShardTestCase.updateRoutingEntry(shard, original);
        final ShardRouting toNode2 = ShardRoutingHelper.relocate(original, "node_2");
        IndexShardTestCase.updateRoutingEntry(shard, toNode2);
        final AtomicBoolean relocated = new AtomicBoolean();
        assertThatThrownBy(() -> shard.relocated(toNode1.getTargetRelocatingShard().allocationId().getId(), ctx -> relocated.set(true)))
            .isExactlyInstanceOf(IllegalStateException.class)
            .hasMessage(
                "relocation target [" + toNode1.getTargetRelocatingShard().allocationId().getId()
                + "] is no longer part of the replication group");
        assertThat(relocated.get()).isFalse();
        shard.relocated(toNode2.getTargetRelocatingShard().allocationId().getId(), ctx -> relocated.set(true));
        assertThat(relocated.get()).isTrue();
        closeShards(shard);
    }

    @Test
    public void testConcurrentAcquireAllReplicaOperationsPermitsWithPrimaryTermUpdate() throws Exception {
        final IndexShard replica = newStartedShard(false);
        indexOnReplicaWithGaps(replica, between(0, 1000), Math.toIntExact(replica.getLocalCheckpoint()), true);

        final int nbTermUpdates = randomIntBetween(1, 5);

        for (int i = 0; i < nbTermUpdates; i++) {
            long opPrimaryTerm = replica.getOperationPrimaryTerm() + 1;
            final long globalCheckpoint = replica.getLastKnownGlobalCheckpoint();
            final long maxSeqNoOfUpdatesOrDeletes = replica.getMaxSeqNoOfUpdatesOrDeletes();

            final int operations = scaledRandomIntBetween(5, 32);
            final CyclicBarrier barrier = new CyclicBarrier(1 + operations);
            final CountDownLatch latch = new CountDownLatch(operations);

            final Thread[] threads = new Thread[operations];
            for (int j = 0; j < operations; j++) {
                threads[j] = new Thread(() -> {
                    try {
                        barrier.await();
                    } catch (final BrokenBarrierException | InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    replica.acquireAllReplicaOperationsPermits(
                        opPrimaryTerm,
                        globalCheckpoint,
                        maxSeqNoOfUpdatesOrDeletes,
                        new ActionListener<Releasable>() {
                            @Override
                            public void onResponse(final Releasable releasable) {
                                try (Releasable ignored = releasable) {
                                    assertThat(replica.getPendingPrimaryTerm()).isGreaterThanOrEqualTo(opPrimaryTerm);
                                    assertThat(replica.getOperationPrimaryTerm()).isEqualTo(opPrimaryTerm);
                                } finally {
                                    latch.countDown();
                                }
                            }

                            @Override
                            public void onFailure(final Exception e) {
                                try {
                                    throw new RuntimeException(e);
                                } finally {
                                    latch.countDown();
                                }
                            }
                        }, TimeValue.timeValueMinutes(30L));
                });
                threads[j].start();
            }
            barrier.await();
            latch.await();

            for (Thread thread : threads) {
                thread.join();
            }
        }

        closeShard(replica, false);
    }

    class Result {
        private final int localCheckpoint;
        private final int maxSeqNo;

        Result(final int localCheckpoint, final int maxSeqNo) {
            this.localCheckpoint = localCheckpoint;
            this.maxSeqNo = maxSeqNo;
        }
    }

    /**
     * Index on the specified shard while introducing sequence number gaps.
     *
     * @param indexShard the shard
     * @param operations the number of operations
     * @param offset     the starting sequence number
     * @return a pair of the maximum sequence number and whether or not a gap was introduced
     * @throws Exception if an I/O exception occurs while indexing on the shard
     */
    private Result indexOnReplicaWithGaps(
            final IndexShard indexShard,
            final int operations,
            final int offset,
            boolean withFlush) throws IOException {
        int localCheckpoint = offset;
        int max = offset;
        boolean gap = false;
        Set<String> ids = new HashSet<>();
        for (int i = offset + 1; i < operations; i++) {
            if (!rarely() || i == operations - 1) { // last operation can't be a gap as it's not a gap anymore
                final String id = ids.isEmpty() || randomBoolean() ? Integer.toString(i) : randomFrom(ids);
                if (ids.add(id) == false) { // this is an update
                    indexShard.advanceMaxSeqNoOfUpdatesOrDeletes(i);
                }
                SourceToParse sourceToParse = new SourceToParse(indexShard.shardId().getIndexName(), id,
                        new BytesArray("{}"), XContentType.JSON);
                indexShard.applyIndexOperationOnReplica(
                    i,
                    primaryTerm,
                    1,
                    -1,
                    false,
                    sourceToParse
                );
                if (!gap && i == localCheckpoint + 1) {
                    localCheckpoint++;
                }
                max = i;
            } else {
                gap = true;
            }
            if (withFlush && rarely()) {
                indexShard.flush(new FlushRequest());
            }
        }
        indexShard.sync(); // advance local checkpoint
        assert localCheckpoint == indexShard.getLocalCheckpoint();
        assert !gap || (localCheckpoint != max);
        return new Result(localCheckpoint, max);
    }

    @Test
    public void testTypelessGet() throws IOException {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata metaData = IndexMetadata.builder("index")
            .putMapping("{ \"properties\": { \"foo\":  { \"type\": \"text\", \"position\": 1}}}")
            .settings(settings)
            .primaryTerm(0, 1).build();
        IndexShard shard = newShard(new ShardId(metaData.getIndex(), 0), true, "n1", metaData, null);
        recoverShardFromStore(shard);
        Engine.IndexResult indexResult = indexDoc(shard, "0", "{\"foo\" : \"bar\"}");
        assertThat(indexResult.isCreated()).isTrue();

        org.elasticsearch.index.engine.Engine.GetResult getResult
            = shard.get(new Engine.Get("0", new Term("_id", Uid.encodeId("0"))));
        assertThat(getResult).isNotEqualTo(Engine.GetResult.NOT_EXISTS);
        getResult.close();

        closeShards(shard);
    }

    @Test
    public void testDoNotTrimCommitsWhenOpenReadOnlyEngine() throws Exception {
        IndexShard shard = newStartedShard(false, Settings.EMPTY, List.of());
        long numDocs = randomLongBetween(1, 20);
        long seqNo = 0;
        for (long i = 0; i < numDocs; i++) {
            if (rarely()) {
                seqNo++; // create gaps in sequence numbers
            }
            shard.applyIndexOperationOnReplica(
                seqNo, primaryTerm, 1, UNSET_AUTO_GENERATED_TIMESTAMP, false,
                new SourceToParse(
                    shard.shardId.getIndexName(),
                    Long.toString(i),
                    new BytesArray("{}"),
                    XContentType.JSON)
            );
            shard.updateGlobalCheckpointOnReplica(shard.getLocalCheckpoint(), "test");
            if (randomInt(100) < 10) {
                shard.flush(new FlushRequest());
            }
            seqNo++;
        }
        shard.flush(new FlushRequest());
        assertThat(shard.docStats().getCount()).isEqualTo(numDocs);
        ShardRouting replicaRouting = shard.routingEntry();
        ShardRouting readonlyShardRouting = newShardRouting(
            replicaRouting.shardId(),
            replicaRouting.currentNodeId(),
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE);
        IndexShard readonlyShard = reinitShard(
            shard,
            readonlyShardRouting,
            shard.indexSettings.getIndexMetadata(),
            List.of(idxSettings -> Optional.of(
                engineConfig ->
                    new ReadOnlyEngine(engineConfig, null, null, true, Function.identity(), true) {

                        @Override
                        protected void ensureMaxSeqNoEqualsToGlobalCheckpoint(SeqNoStats seqNoStats) {
                            // just like a following shard, we need to skip this check for now.
                        }
                    }
            ))
        );
        DiscoveryNode localNode = new DiscoveryNode(
            "foo",
            buildNewFakeTransportAddress(),
            Map.of(),
            Set.of(),
            Version.CURRENT);
        readonlyShard.markAsRecovering("store", new RecoveryState(readonlyShard.routingEntry(), localNode, null));
        recoverFromStore(readonlyShard);
        assertThat(readonlyShard.docStats().getCount()).isEqualTo(numDocs);
        closeShards(readonlyShard);
    }

    private Releasable acquirePrimaryOperationPermitBlockingly(IndexShard indexShard) throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquirePrimaryOperationPermit(fut, ThreadPool.Names.WRITE, "");
        return fut.get();
    }

    private Releasable acquireReplicaOperationPermitBlockingly(IndexShard indexShard, long opPrimaryTerm)
        throws ExecutionException, InterruptedException {
        PlainActionFuture<Releasable> fut = new PlainActionFuture<>();
        indexShard.acquireReplicaOperationPermit(
            opPrimaryTerm, indexShard.getLastKnownGlobalCheckpoint(),
            randomNonNegativeLong(), fut, ThreadPool.Names.WRITE, "");
        return fut.get();
    }

    private AllocationId randomAllocationId() {
        AllocationId allocationId = AllocationId.newInitializing();
        if (randomBoolean()) {
            allocationId = AllocationId.newRelocation(allocationId);
        }
        return allocationId;
    }

    private ShardStateMetadata getShardStateMetadata(IndexShard shard) {
        ShardRouting shardRouting = shard.routingEntry();
        if (shardRouting == null) {
            return null;
        } else {
            return new ShardStateMetadata(
                shardRouting.primary(),
                shard.indexSettings().getUUID(),
                shardRouting.allocationId());
        }
    }
}
