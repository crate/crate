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

import static io.crate.testing.TestingHelpers.createNodeContext;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.Version;
import org.elasticsearch.action.resync.ResyncReplicationRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.translog.TestTranslog;
import org.elasticsearch.index.translog.Translog;
import org.junit.Test;
import org.mockito.Mockito;

import io.crate.execution.dml.IndexItem;
import io.crate.execution.dml.Indexer;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;

public class PrimaryReplicaSyncerTests extends IndexShardTestCase {

    @Test
    public void testSyncerSendsOffCorrectDocuments() throws Exception {
        IndexShard shard = newStartedShard(true);
        AtomicBoolean syncActionCalled = new AtomicBoolean();
        List<ResyncReplicationRequest> resyncRequests = new ArrayList<>();
        PrimaryReplicaSyncer.SyncAction syncAction =
            (request, allocationId, primaryTerm, listener) -> {
                logger.info("Sending off {} operations", request.getOperations().length);
                syncActionCalled.set(true);
                resyncRequests.add(request);
                listener.onResponse(new ReplicationResponse());
            };
        PrimaryReplicaSyncer syncer = new PrimaryReplicaSyncer(syncAction);
        syncer.setChunkSize(new ByteSizeValue(randomIntBetween(1, 10)));


        int numDocs = randomInt(10);
        Indexer indexer = createIndexer(shard, List.of());
        for (int i = 0; i < numDocs; i++) {
            // Index doc but not advance local checkpoint.
            var item = new IndexItem.StaticItem(Integer.toString(i), List.of(), new Object[]{}, 0L, 0L);
            long startTime = System.nanoTime();
            ParsedDocument parsedDocument = indexer.index(item);
            shard.applyIndexOperationOnPrimary(parsedDocument, Versions.MATCH_ANY, startTime, -1L, true);
        }

        long globalCheckPoint = numDocs > 0 ? randomIntBetween(0, numDocs - 1) : 0;
        boolean syncNeeded = numDocs > 0;

        String allocationId = shard.routingEntry().allocationId().getId();
        shard.updateShardState(
            shard.routingEntry(),
            shard.getPendingPrimaryTerm(),
            null,
            1000L,
            Collections.singleton(allocationId),
            new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard.routingEntry()).build()
        );
        shard.updateLocalCheckpointForShard(allocationId, globalCheckPoint);
        assertEquals(globalCheckPoint, shard.getLastKnownGlobalCheckpoint());

        logger.info("Total ops: {}, global checkpoint: {}", numDocs, globalCheckPoint);

        PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<>();
        syncer.resync(shard, fut);
        PrimaryReplicaSyncer.ResyncTask resyncTask = fut.get();

        if (syncNeeded) {
            assertTrue("Sync action was not called", syncActionCalled.get());
            ResyncReplicationRequest resyncRequest = resyncRequests.remove(0);
            assertThat(resyncRequest.getTrimAboveSeqNo(), equalTo(numDocs - 1L));

            assertThat("trimAboveSeqNo has to be specified in request #0 only", resyncRequests.stream()
                    .mapToLong(ResyncReplicationRequest::getTrimAboveSeqNo)
                    .filter(seqNo -> seqNo != SequenceNumbers.UNASSIGNED_SEQ_NO)
                    .findFirst()
                    .isPresent(),
                is(false));

            assertThat(resyncRequest.getMaxSeenAutoIdTimestampOnPrimary(), equalTo(shard.getMaxSeenAutoIdTimestamp()));
        }
        if (syncNeeded && globalCheckPoint < numDocs - 1) {
            assertThat(resyncTask.getSkippedOperations(), equalTo(0));
            assertThat(resyncTask.getResyncedOperations(), equalTo(Math.toIntExact(numDocs - 1 - globalCheckPoint)));
            if (shard.indexSettings.isSoftDeleteEnabled()) {
                assertThat(resyncTask.getTotalOperations(), equalTo(Math.toIntExact(numDocs - 1 - globalCheckPoint)));
            } else {
                assertThat(resyncTask.getTotalOperations(), equalTo(numDocs));
            }
         } else {
             assertThat(resyncTask.getSkippedOperations(), equalTo(0));
             assertThat(resyncTask.getResyncedOperations(), equalTo(0));
             assertThat(resyncTask.getTotalOperations(), equalTo(0));
         }
        closeShards(shard);
    }

    public void testSyncerOnClosingShard() throws Exception {
        IndexShard shard = newStartedShard(true);
        AtomicBoolean syncActionCalled = new AtomicBoolean();
        PrimaryReplicaSyncer.SyncAction syncAction =
            (request, allocationId, primaryTerm, listener) -> {
                logger.info("Sending off {} operations", request.getOperations().length);
                syncActionCalled.set(true);
                threadPool.generic().execute(() -> listener.onResponse(new ReplicationResponse()));
            };
        PrimaryReplicaSyncer syncer = new PrimaryReplicaSyncer(syncAction);
        syncer.setChunkSize(new ByteSizeValue(1)); // every document is sent off separately

        int numDocs = 10;
        Indexer indexer = createIndexer(shard, List.of());
        for (int i = 0; i < numDocs; i++) {
            // Index doc but not advance local checkpoint.

            var item = new IndexItem.StaticItem(Integer.toString(i), List.of(), new Object[]{}, 0L, 0L);
            long startTime = System.nanoTime();
            ParsedDocument parsedDocument = indexer.index(item);
            shard.applyIndexOperationOnPrimary(parsedDocument, Versions.MATCH_ANY, startTime, -1L, false);
        }

        String allocationId = shard.routingEntry().allocationId().getId();
        shard.updateShardState(
            shard.routingEntry(),
            shard.getPendingPrimaryTerm(),
            null,
            1000L,
            Collections.singleton(allocationId),
            new IndexShardRoutingTable.Builder(shard.shardId()).addShard(shard.routingEntry()).build()
        );

        CountDownLatch syncCalledLatch = new CountDownLatch(1);
        PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<PrimaryReplicaSyncer.ResyncTask>() {
            @Override
            public void onFailure(Exception e) {
                try {
                    super.onFailure(e);
                } finally {
                    syncCalledLatch.countDown();
                }
            }
            @Override
            public void onResponse(PrimaryReplicaSyncer.ResyncTask result) {
                try {
                    super.onResponse(result);
                } finally {
                    syncCalledLatch.countDown();
                }
            }
        };
        threadPool.generic().execute(() -> {
            syncer.resync(shard, fut);
        });
        if (randomBoolean()) {
            syncCalledLatch.await();
        }
        closeShards(shard);
        try {
            FutureUtils.get(fut);
            assertTrue("Sync action was not called", syncActionCalled.get());
        } catch (AlreadyClosedException | IndexShardClosedException ignored) {
            // ignore
        }
    }

    @Test
    public void testDoNotSendOperationsWithoutSequenceNumber() throws Exception {
        IndexShard shard = Mockito.spy(newStartedShard(true));
        Mockito.when(shard.getLastKnownGlobalCheckpoint()).thenReturn(SequenceNumbers.UNASSIGNED_SEQ_NO);
        int numOps = between(0, 20);
        List<Translog.Operation> operations = new ArrayList<>();
        for (int i = 0; i < numOps; i++) {
            operations.add(new Translog.Index(
                Integer.toString(i), randomBoolean() ? SequenceNumbers.UNASSIGNED_SEQ_NO : i, primaryTerm, new byte[]{1}));
        }
        Engine.HistorySource source =
            shard.indexSettings.isSoftDeleteEnabled() ? Engine.HistorySource.INDEX : Engine.HistorySource.TRANSLOG;
        doReturn(TestTranslog.newSnapshotFromOperations(operations)).when(shard).getHistoryOperations(anyString(), eq(source), anyLong());
        List<Translog.Operation> sentOperations = new ArrayList<>();
        PrimaryReplicaSyncer.SyncAction syncAction = (request, allocationId, primaryTerm, listener) -> {
            sentOperations.addAll(Arrays.asList(request.getOperations()));
            listener.onResponse(new ReplicationResponse());
        };
        PrimaryReplicaSyncer syncer = new PrimaryReplicaSyncer(syncAction);
        syncer.setChunkSize(new ByteSizeValue(randomIntBetween(1, 10)));
        PlainActionFuture<PrimaryReplicaSyncer.ResyncTask> fut = new PlainActionFuture<>();
        syncer.resync(shard, fut);
        FutureUtils.get(fut);
        assertThat(sentOperations, equalTo(operations.stream().filter(op -> op.seqNo() >= 0).collect(Collectors.toList())));
        closeShards(shard);
    }

    private static Indexer createIndexer(IndexShard shard, List<Reference> targetColumns) {
        DocTableInfo table = new DocTableInfo(
            new RelationName("doc", shard.shardId().getIndexName()),
            targetColumns.stream().collect(Collectors.toMap(Reference::column, r -> r)),
            Map.of(),
            Map.of(),
            null,
            List.of(),
            List.of(),
            null,
            new String[] { shard.shardId().getIndexName() },
            new String[] { shard.shardId().getIndexName() },
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .build(),
            List.of(),
            List.of(),
            ColumnPolicy.STRICT,
            Version.CURRENT,
            null,
            false,
            Set.of()
        );
        return new Indexer(
            shard.shardId().getIndexName(),
            table,
            CoordinatorTxnCtx.systemTransactionContext(),
            createNodeContext(),
            column -> shard.mapperService().getLuceneFieldType(column),
            targetColumns,
            null
        );
    }
}
