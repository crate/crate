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

package org.elasticsearch.index.replication;

import io.crate.common.io.IOUtils;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngineTests;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.PeerRecoveryTargetService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryTarget;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RecoveryDuringReplicationTests extends ESIndexLevelReplicationTestCase {

    public void testIndexingDuringFileRecovery() throws Exception {
        try (ReplicationGroup shards = createGroup(randomInt(1))) {
            shards.startAll();
            int docs = shards.indexDocs(randomInt(50));
            shards.flush();
            IndexShard replica = shards.addReplica();
            final CountDownLatch recoveryBlocked = new CountDownLatch(1);
            final CountDownLatch releaseRecovery = new CountDownLatch(1);
            final RecoveryState.Stage blockOnStage = randomFrom(BlockingTarget.SUPPORTED_STAGES);
            final Future<Void> recoveryFuture = shards.asyncRecoverReplica(replica, (indexShard, node) ->
                new BlockingTarget(blockOnStage, recoveryBlocked, releaseRecovery, indexShard, node, recoveryListener, logger));

            recoveryBlocked.await();
            docs += shards.indexDocs(randomInt(20));
            releaseRecovery.countDown();
            recoveryFuture.get();

            shards.assertAllEqual(docs);
        }
    }

    public void testRecoveryOfDisconnectedReplica() throws Exception {
        Settings settings = Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false).build();
        try (ReplicationGroup shards = createGroup(1, settings)) {
            shards.startAll();
            int docs = shards.indexDocs(randomInt(50));
            shards.flush();
            final IndexShard originalReplica = shards.getReplicas().get(0);
            for (int i = 0; i < randomInt(2); i++) {
                final int indexedDocs = shards.indexDocs(randomInt(5));
                docs += indexedDocs;

                final boolean flush = randomBoolean();
                if (flush) {
                    originalReplica.flush(new FlushRequest());
                }
            }

            // simulate a background global checkpoint sync at which point we expect the global checkpoint to advance on the replicas
            shards.syncGlobalCheckpoint();
            long globalCheckpointOnReplica = originalReplica.getLastSyncedGlobalCheckpoint();
            Optional<SequenceNumbers.CommitInfo> safeCommitOnReplica =
                originalReplica.store().findSafeIndexCommit(globalCheckpointOnReplica);
            assertTrue(safeCommitOnReplica.isPresent());
            shards.removeReplica(originalReplica);

            final int missingOnReplica = shards.indexDocs(randomInt(5));
            docs += missingOnReplica;

            final boolean translogTrimmed;
            if (randomBoolean()) {
                shards.flush();
                translogTrimmed = randomBoolean();
                if (translogTrimmed) {
                    final Translog translog = getTranslog(shards.getPrimary());
                    translog.getDeletionPolicy().setRetentionAgeInMillis(0);
                    translog.trimUnreferencedReaders();
                }
            } else {
                translogTrimmed = false;
            }
            originalReplica.close("disconnected", false);
            IOUtils.close(originalReplica.store());
            final IndexShard recoveredReplica =
                shards.addReplicaWithExistingPath(originalReplica.shardPath(), originalReplica.routingEntry().currentNodeId());
            shards.recoverReplica(recoveredReplica);
            if (translogTrimmed && missingOnReplica > 0) {
                // replica has something to catch up with, but since we trimmed the primary translog, we should fall back to full recovery
                assertThat(recoveredReplica.recoveryState().getIndex().fileDetails().isEmpty(), is(false));
            } else {
                assertThat(recoveredReplica.recoveryState().getIndex().fileDetails().entrySet(), is(empty()));
                assertThat(recoveredReplica.recoveryState().getTranslog().recoveredOperations(),
                           equalTo(Math.toIntExact(docs - 1 - safeCommitOnReplica.get().localCheckpoint)));
                assertThat(recoveredReplica.recoveryState().getTranslog().totalLocal(),
                           equalTo(Math.toIntExact(globalCheckpointOnReplica - safeCommitOnReplica.get().localCheckpoint)));
            }

            docs += shards.indexDocs(randomInt(5));

            shards.assertAllEqual(docs);
        }
    }

    /*
     * Simulate a scenario with two replicas where one of the replicas receives an extra document, the other replica is promoted on primary
     * failure, the receiving replica misses the primary/replica re-sync and then recovers from the primary. We expect that a
     * sequence-number based recovery is performed and the extra document does not remain after recovery.
     */
    public void testRecoveryToReplicaThatReceivedExtraDocument() throws Exception {
        try (ReplicationGroup shards = createGroup(2)) {
            shards.startAll();
            final int docs = randomIntBetween(0, 16);
            shards.indexDocs(docs);

            shards.flush();
            shards.syncGlobalCheckpoint();

            final IndexShard oldPrimary = shards.getPrimary();
            final IndexShard promotedReplica = shards.getReplicas().get(0);
            final IndexShard remainingReplica = shards.getReplicas().get(1);
            // slip the extra document into the replica
            remainingReplica.applyIndexOperationOnReplica(
                remainingReplica.getLocalCheckpoint() + 1,
                remainingReplica.getOperationPrimaryTerm(),
                1,
                randomNonNegativeLong(),
                false,
                new SourceToParse("index", "replica", new BytesArray("{}"), XContentType.JSON));
            shards.promoteReplicaToPrimary(promotedReplica).get();
            oldPrimary.close("demoted", randomBoolean());
            oldPrimary.store().close();
            shards.removeReplica(remainingReplica);
            remainingReplica.close("disconnected", false);
            remainingReplica.store().close();
            // randomly introduce a conflicting document
            final boolean extra = randomBoolean();
            if (extra) {
                promotedReplica.applyIndexOperationOnPrimary(
                    Versions.MATCH_ANY,
                    VersionType.INTERNAL,
                    new SourceToParse("index", "primary", new BytesArray("{}"), XContentType.JSON),
                    SequenceNumbers.UNASSIGNED_SEQ_NO, 0, -1L,
                    false);
            }
            final IndexShard recoveredReplica =
                shards.addReplicaWithExistingPath(remainingReplica.shardPath(), remainingReplica.routingEntry().currentNodeId());
            shards.recoverReplica(recoveredReplica);

            assertThat(recoveredReplica.recoveryState().getIndex().fileDetails().isEmpty(), is(true));
            assertThat(recoveredReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(extra ? 1 : 0));

            shards.assertAllEqual(docs + (extra ? 1 : 0));
        }
    }


    public static class BlockingTarget extends RecoveryTarget {

        private final CountDownLatch recoveryBlocked;
        private final CountDownLatch releaseRecovery;
        private final RecoveryState.Stage stageToBlock;
        static final EnumSet<RecoveryState.Stage> SUPPORTED_STAGES =
            EnumSet.of(RecoveryState.Stage.INDEX, RecoveryState.Stage.TRANSLOG, RecoveryState.Stage.FINALIZE);
        private final Logger logger;

        public BlockingTarget(RecoveryState.Stage stageToBlock, CountDownLatch recoveryBlocked, CountDownLatch releaseRecovery,
                              IndexShard shard, DiscoveryNode sourceNode, PeerRecoveryTargetService.RecoveryListener listener,
                              Logger logger) {
            super(shard, sourceNode, listener);
            this.recoveryBlocked = recoveryBlocked;
            this.releaseRecovery = releaseRecovery;
            this.stageToBlock = stageToBlock;
            this.logger = logger;
            if (SUPPORTED_STAGES.contains(stageToBlock) == false) {
                throw new UnsupportedOperationException(stageToBlock + " is not supported");
            }
        }

        private boolean hasBlocked() {
            return recoveryBlocked.getCount() == 0;
        }

        private void blockIfNeeded(RecoveryState.Stage currentStage) {
            if (currentStage == stageToBlock) {
                logger.info("--> blocking recovery on stage [{}]", currentStage);
                recoveryBlocked.countDown();
                try {
                    releaseRecovery.await();
                    logger.info("--> recovery continues from stage [{}]", currentStage);
                } catch (InterruptedException e) {
                    throw new RuntimeException("blockage released");
                }
            }
        }

        @Override
        public void indexTranslogOperations(
            final List<Translog.Operation> operations,
            final int totalTranslogOps,
            final long maxAutoIdTimestamp,
            final long maxSeqNoOfUpdates,
            final RetentionLeases retentionLeases,
            final long mappingVersion,
            final ActionListener<Long> listener) {
            if (hasBlocked() == false) {
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            super.indexTranslogOperations(
                operations, totalTranslogOps, maxAutoIdTimestamp, maxSeqNoOfUpdates, retentionLeases, mappingVersion, listener);
        }

        @Override
        public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                               ActionListener<Void> listener) {
            blockIfNeeded(RecoveryState.Stage.INDEX);
            super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetadata, listener);
        }

        @Override
        public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
            if (hasBlocked() == false) {
                // it maybe that not ops have been transferred, block now
                blockIfNeeded(RecoveryState.Stage.TRANSLOG);
            }
            blockIfNeeded(RecoveryState.Stage.FINALIZE);
            super.finalizeRecovery(globalCheckpoint, trimAboveSeqNo, listener);
        }

    }

    static class BlockingEngineFactory implements EngineFactory, AutoCloseable {

        private final List<CountDownLatch> blocks = new ArrayList<>();

        private final AtomicReference<CountDownLatch> blockReference = new AtomicReference<>();
        private final AtomicReference<CountDownLatch> blockedIndexers = new AtomicReference<>();

        public synchronized void latchIndexers(int count) {
            final CountDownLatch block = new CountDownLatch(1);
            blocks.add(block);
            blockedIndexers.set(new CountDownLatch(count));
            assert blockReference.compareAndSet(null, block);
        }

        public void awaitIndexersLatch() throws InterruptedException {
            blockedIndexers.get().await();
        }

        public synchronized void allowIndexing() {
            final CountDownLatch previous = blockReference.getAndSet(null);
            assert previous == null || blocks.contains(previous);
        }

        public synchronized void releaseLatchedIndexers() {
            allowIndexing();
            blocks.forEach(CountDownLatch::countDown);
            blocks.clear();
        }

        @Override
        public Engine newReadWriteEngine(final EngineConfig config) {
            return InternalEngineTests.createInternalEngine(
                (directory, writerConfig) ->
                    new IndexWriter(directory, writerConfig) {
                        @Override
                        public long addDocument(final Iterable<? extends IndexableField> doc) throws IOException {
                            final CountDownLatch block = blockReference.get();
                            if (block != null) {
                                final CountDownLatch latch = blockedIndexers.get();
                                if (latch != null) {
                                    latch.countDown();
                                }
                                try {
                                    block.await();
                                } catch (InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                            }
                            return super.addDocument(doc);
                        }
                    },
                null,
                null,
                config);
        }

        @Override
        public void close() throws Exception {
            releaseLatchedIndexers();
        }

    }

}
