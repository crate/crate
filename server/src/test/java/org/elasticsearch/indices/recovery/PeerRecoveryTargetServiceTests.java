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

package org.elasticsearch.indices.recovery;

import static org.assertj.core.api.Assertions.assertThat;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.NoOpEngine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.junit.Test;

import io.crate.common.io.IOUtils;

public class PeerRecoveryTargetServiceTests extends IndexShardTestCase {

    @Test
    public void testWriteFileChunksConcurrently() throws Exception {
        IndexShard sourceShard = newStartedShard(true);
        int numDocs = between(20, 100);
        for (int i = 0; i < numDocs; i++) {
            indexDoc(sourceShard, "_doc", Integer.toString(i));
        }
        sourceShard.flush(new FlushRequest());
        Store.MetadataSnapshot sourceSnapshot = sourceShard.store().getMetadata(null);
        List<StoreFileMetadata> mdFiles = new ArrayList<>();
        for (StoreFileMetadata md : sourceSnapshot) {
            mdFiles.add(md);
        }
        final IndexShard targetShard = newShard(false);
        final DiscoveryNode pNode = getFakeDiscoNode(sourceShard.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(targetShard.routingEntry().currentNodeId());
        targetShard.markAsRecovering("test-peer-recovery", new RecoveryState(targetShard.routingEntry(), rNode, pNode));
        final RecoveryTarget recoveryTarget = new RecoveryTarget(targetShard, null, null);
        final PlainActionFuture<Void> receiveFileInfoFuture = new PlainActionFuture<>();
        recoveryTarget.receiveFileInfo(
            mdFiles.stream().map(StoreFileMetadata::name).collect(Collectors.toList()),
            mdFiles.stream().map(StoreFileMetadata::length).collect(Collectors.toList()),
            Collections.emptyList(), Collections.emptyList(), 0, receiveFileInfoFuture
        );
        FutureUtils.get(receiveFileInfoFuture, (long) 5, TimeUnit.SECONDS);
        List<RecoveryFileChunkRequest> requests = new ArrayList<>();
        long seqNo = 0;
        for (StoreFileMetadata md : mdFiles) {
            try (IndexInput in = sourceShard.store().directory().openInput(md.name(), IOContext.READONCE)) {
                int pos = 0;
                while (pos < md.length()) {
                    int length = between(1, Math.toIntExact(md.length() - pos));
                    byte[] buffer = new byte[length];
                    in.readBytes(buffer, 0, length);
                    requests.add(new RecoveryFileChunkRequest(0, seqNo++, sourceShard.shardId(), md, pos, new BytesArray(buffer),
                        pos + length == md.length(), 1, 1));
                    pos += length;
                }
            }
        }
        Randomness.shuffle(requests);
        BlockingQueue<RecoveryFileChunkRequest> queue = new ArrayBlockingQueue<>(requests.size());
        queue.addAll(requests);
        Thread[] senders = new Thread[between(1, 4)];
        CyclicBarrier barrier = new CyclicBarrier(senders.length);
        for (int i = 0; i < senders.length; i++) {
            senders[i] = new Thread(() -> {
                try {
                    barrier.await();
                    RecoveryFileChunkRequest r;
                    while ((r = queue.poll()) != null) {
                        recoveryTarget.writeFileChunk(
                            r.metadata(),
                            r.position(),
                            r.content(),
                            r.lastChunk(),
                            r.totalTranslogOps(),
                            ActionListener.wrap(
                                ignored -> {
                                }, e -> {
                                    throw new AssertionError(e);
                                }));
                    }
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            });
            senders[i].start();
        }
        for (Thread sender : senders) {
            sender.join();
        }
        PlainActionFuture<Void> cleanFilesFuture = new PlainActionFuture<>();
        recoveryTarget.cleanFiles(0, Long.parseLong(sourceSnapshot.getCommitUserData().get(SequenceNumbers.MAX_SEQ_NO)),
            sourceSnapshot, cleanFilesFuture);
        FutureUtils.get(cleanFilesFuture);
        recoveryTarget.decRef();
        Store.MetadataSnapshot targetSnapshot = targetShard.snapshotStoreMetadata();
        Store.RecoveryDiff diff = sourceSnapshot.recoveryDiff(targetSnapshot);
        assertThat(diff.different, empty());
        closeShards(sourceShard, targetShard);
    }

    private SeqNoStats populateRandomData(IndexShard shard) throws IOException {
        List<Long> seqNos = LongStream.range(0, 100).boxed().collect(Collectors.toList());
        Randomness.shuffle(seqNos);
        for (long seqNo : seqNos) {
            shard.applyIndexOperationOnReplica(
                seqNo,
                1,
                shard.getOperationPrimaryTerm(),
                Translog.UNSET_AUTO_GENERATED_TIMESTAMP,
                false, new SourceToParse(shard.shardId().getIndexName(), UUIDs.randomBase64UUID(), new BytesArray("{}"), XContentType.JSON)
            );
            if (randomInt(100) < 5) {
                shard.flush(new FlushRequest().waitIfOngoing(true));
            }
        }
        shard.sync();
        long globalCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, shard.getLocalCheckpoint());
        shard.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");
        shard.sync();
        return shard.seqNoStats();
    }

    @Test
    public void testPrepareIndexForPeerRecovery() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        // empty copy
        IndexShard shard = newShard(false);
        shard.markAsRecovering("for testing", new RecoveryState(shard.routingEntry(), localNode, localNode));
        shard.prepareForIndexRecovery();
        assertThat(shard.recoverLocallyUpToGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(shard.recoveryState().getTranslog().totalLocal()).isEqualTo(RecoveryState.Translog.UNKNOWN);
        assertThat(shard.recoveryState().getTranslog().recoveredOperations()).isEqualTo(0);
        assertThat(shard.getLastKnownGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        closeShards(shard);

        // good copy
        shard = newStartedShard(false);
        long globalCheckpoint = populateRandomData(shard).getGlobalCheckpoint();
        Optional<SequenceNumbers.CommitInfo> safeCommit = shard.store().findSafeIndexCommit(globalCheckpoint);
        assertTrue(safeCommit.isPresent());
        int expectedTotalLocal = 0;
        if (safeCommit.get().localCheckpoint < globalCheckpoint) {
            try (Translog.Snapshot snapshot = getTranslog(shard).newSnapshot(safeCommit.get().localCheckpoint + 1, globalCheckpoint)) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    if (op.seqNo() <= globalCheckpoint) {
                        expectedTotalLocal++;
                    }
                }
            }
        }
        IndexShard replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(replica.recoverLocallyUpToGlobalCheckpoint()).isEqualTo(globalCheckpoint + 1);
        assertThat(replica.recoveryState().getTranslog().totalLocal()).isEqualTo(expectedTotalLocal);
        assertThat(replica.recoveryState().getTranslog().recoveredOperations()).isEqualTo(expectedTotalLocal);
        assertThat(replica.getLastKnownGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        closeShards(replica);

        // corrupted copy
        shard = newStartedShard(false);
        if (randomBoolean()) {
            populateRandomData(shard);
        }
        shard.store().markStoreCorrupted(new IOException("test"));
        replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(replica.recoverLocallyUpToGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(replica.recoveryState().getTranslog().totalLocal()).isEqualTo(RecoveryState.Translog.UNKNOWN);
        assertThat(replica.recoveryState().getTranslog().recoveredOperations()).isEqualTo(0);
        assertThat(replica.getLastKnownGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        closeShards(replica);

        // copy with truncated translog
        shard = newStartedShard(false);
        SeqNoStats seqNoStats = populateRandomData(shard);
        replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
            RecoverySource.PeerRecoverySource.INSTANCE));
        globalCheckpoint =  randomFrom(UNASSIGNED_SEQ_NO, seqNoStats.getMaxSeqNo());
        String translogUUID = Translog.createEmptyTranslog(replica.shardPath().resolveTranslog(), globalCheckpoint,
            replica.shardId(), replica.getPendingPrimaryTerm());
        replica.store().associateIndexWithNewTranslog(translogUUID);
        safeCommit = replica.store().findSafeIndexCommit(globalCheckpoint);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        if (safeCommit.isPresent()) {
            assertThat(replica.recoverLocallyUpToGlobalCheckpoint()).isEqualTo(safeCommit.get().localCheckpoint + 1);
            assertThat(replica.recoveryState().getTranslog().totalLocal()).isEqualTo(0);
        } else {
            assertThat(replica.recoverLocallyUpToGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
            assertThat(replica.recoveryState().getTranslog().totalLocal()).isEqualTo(RecoveryState.Translog.UNKNOWN);
        }
        assertThat(replica.recoveryState().getStage()).isEqualTo(RecoveryState.Stage.TRANSLOG);
        assertThat(replica.recoveryState().getTranslog().recoveredOperations()).isEqualTo(0);
        assertThat(replica.getLastKnownGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(replica.recoveryState().getStage()).isEqualTo(RecoveryState.Stage.TRANSLOG);
        closeShards(replica);
    }

    @Test
    public void test_prepare_old_index_for_peer_recovery_without_safe_commit() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
                                                    Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);

        var shard = newStartedShard(
            false,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_3_0_1).build()
        );
        populateRandomData(shard);
        var globalCheckpoint = UNASSIGNED_SEQ_NO;
        var replica = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(),
                                                                       RecoverySource.PeerRecoverySource.INSTANCE));
        String translogUUID = Translog.createEmptyTranslog(replica.shardPath().resolveTranslog(), globalCheckpoint,
                                                           replica.shardId(), replica.getPendingPrimaryTerm());
        replica.store().associateIndexWithNewTranslog(translogUUID);
        var safeCommit = replica.store().findSafeIndexCommit(globalCheckpoint);
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(safeCommit.isPresent()).isFalse();
        assertThat(replica.recoverLocallyUpToGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(replica.recoveryState().getTranslog().totalLocal()).isEqualTo(RecoveryState.Translog.UNKNOWN);
        assertThat(replica.recoveryState().getTranslog().recoveredOperations()).isEqualTo(0);
        assertThat(replica.getLastKnownGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(replica.recoveryState().getStage()).isEqualTo(RecoveryState.Stage.TRANSLOG);
        closeShards(replica);
    }

    @Test
    public void testClosedIndexSkipsLocalRecovery() throws Exception {
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        IndexShard shard = newStartedShard(false);
        long globalCheckpoint = populateRandomData(shard).getGlobalCheckpoint();
        Optional<SequenceNumbers.CommitInfo> safeCommit = shard.store().findSafeIndexCommit(globalCheckpoint);
        assertTrue(safeCommit.isPresent());
        final IndexMetadata indexMetadata;
        if (randomBoolean()) {
            indexMetadata = IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
                .settings(shard.indexSettings().getSettings())
                .state(IndexMetadata.State.CLOSE).build();
        } else {
            indexMetadata = IndexMetadata.builder(shard.indexSettings().getIndexMetadata())
                .settings(Settings.builder().put(shard.indexSettings().getSettings())
                    .put(IndexMetadata.SETTING_BLOCKS_WRITE, true)).build();
        }
        IndexShard replica = reinitShard(
            shard,
            ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE),
            indexMetadata,
            List.of(idxSettings -> Optional.of(NoOpEngine::new))
        );
        replica.markAsRecovering("for testing", new RecoveryState(replica.routingEntry(), localNode, localNode));
        replica.prepareForIndexRecovery();
        assertThat(replica.recoverLocallyUpToGlobalCheckpoint()).isEqualTo(safeCommit.get().localCheckpoint + 1);
        assertThat(replica.recoveryState().getTranslog().totalLocal()).isEqualTo(0);
        assertThat(replica.recoveryState().getTranslog().recoveredOperations()).isEqualTo(0);
        assertThat(replica.getLastKnownGlobalCheckpoint()).isEqualTo(UNASSIGNED_SEQ_NO);
        closeShards(replica);
    }

    @Test
    public void testResetStartingSeqNoIfLastCommitCorrupted() throws Exception {
        IndexShard shard = newStartedShard(false);
        populateRandomData(shard);
        DiscoveryNode pNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        DiscoveryNode rNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        shard = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE));
        shard.markAsRecovering("peer recovery", new RecoveryState(shard.routingEntry(), pNode, rNode));
        shard.prepareForIndexRecovery();
        long startingSeqNo = shard.recoverLocallyUpToGlobalCheckpoint();
        shard.store().markStoreCorrupted(new IOException("simulated"));
        RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, null);
        StartRecoveryRequest request = PeerRecoveryTargetService.getStartRecoveryRequest(logger, rNode, recoveryTarget, startingSeqNo);
        assertThat(request.startingSeqNo()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(request.metadataSnapshot().size()).isEqualTo(0);
        recoveryTarget.decRef();
        closeShards(shard);
    }

    @Test
    public void testResetStartRequestIfTranslogIsCorrupted() throws Exception {
        DiscoveryNode pNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        DiscoveryNode rNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(),
            Collections.emptyMap(), Collections.emptySet(), Version.CURRENT);
        IndexShard shard = newStartedShard(false);
        final SeqNoStats seqNoStats = populateRandomData(shard);
        shard.close("test", false);
        if (randomBoolean()) {
            shard.store().associateIndexWithNewTranslog(UUIDs.randomBase64UUID());
        } else if (randomBoolean()) {
            Translog.createEmptyTranslog(
                shard.shardPath().resolveTranslog(), seqNoStats.getGlobalCheckpoint(), shard.shardId(), shard.getOperationPrimaryTerm());
        } else {
            IOUtils.rm(shard.shardPath().resolveTranslog());
        }
        shard = reinitShard(shard, ShardRoutingHelper.initWithSameId(shard.routingEntry(), RecoverySource.PeerRecoverySource.INSTANCE));
        shard.markAsRecovering("peer recovery", new RecoveryState(shard.routingEntry(), pNode, rNode));
        shard.prepareForIndexRecovery();
        RecoveryTarget recoveryTarget = new RecoveryTarget(shard, null, null);
        StartRecoveryRequest request = PeerRecoveryTargetService.getStartRecoveryRequest(
            logger, rNode, recoveryTarget, randomNonNegativeLong());
        assertThat(request.startingSeqNo()).isEqualTo(UNASSIGNED_SEQ_NO);
        assertThat(request.metadataSnapshot(), sameInstance(Store.MetadataSnapshot.EMPTY));
        recoveryTarget.decRef();
        closeShards(shard);
    }
}
