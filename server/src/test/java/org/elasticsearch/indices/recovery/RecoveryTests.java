/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package org.elasticsearch.indices.recovery;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.replication.ESIndexLevelReplicationTestCase;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.SnapshotMatchers;
import org.elasticsearch.index.translog.Translog;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RecoveryTests extends ESIndexLevelReplicationTestCase {

    @Test
    public void testTranslogHistoryTransferred() throws Exception {
        try (ReplicationGroup shards = createGroup(0)) {
            shards.startPrimary();
            int docs = shards.indexDocs(10);
            getTranslog(shards.getPrimary()).rollGeneration();
            shards.flush();
            int moreDocs = shards.indexDocs(randomInt(10));
            shards.addReplica();
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            boolean softDeletesEnabled = replica.indexSettings().isSoftDeleteEnabled();
            int actual = getTranslog(replica).totalOperations();
            assertThat(actual, equalTo(softDeletesEnabled ? 0 : docs + moreDocs));
            shards.assertAllEqual(docs + moreDocs);
        }
    }

    public void testSequenceBasedRecoveryKeepsTranslog() throws Exception {
        try (ReplicationGroup shards = createGroup(1)) {
            shards.startAll();
            final IndexShard replica = shards.getReplicas().get(0);
            final int initDocs = scaledRandomIntBetween(0, 20);
            int uncommittedDocs = 0;
            for (int i = 0; i < initDocs; i++) {
                shards.indexDocs(1);
                uncommittedDocs++;
                if (randomBoolean()) {
                    shards.syncGlobalCheckpoint();
                    shards.flush();
                    uncommittedDocs = 0;
                }
            }
            shards.removeReplica(replica);
            final int moreDocs = shards.indexDocs(scaledRandomIntBetween(0, 20));
            if (randomBoolean()) {
                shards.flush();
            }
            replica.close("test", randomBoolean());
            replica.store().close();
            final IndexShard newReplica = shards.addReplicaWithExistingPath(replica.shardPath(), replica.routingEntry().currentNodeId());
            shards.recoverReplica(newReplica);

            Translog translog = getTranslog(newReplica);
            try (Translog.Snapshot snapshot = translog.newSnapshot()) {
                if (newReplica.indexSettings().isSoftDeleteEnabled()) {
                    assertThat(snapshot.totalOperations(), equalTo(0));
                } else {
                    assertThat("Sequence based recovery should keep existing translog", snapshot, SnapshotMatchers.size(initDocs + moreDocs));
                }
            }
            assertThat(newReplica.recoveryState().getTranslog().recoveredOperations(), equalTo(uncommittedDocs + moreDocs));
            assertThat(newReplica.recoveryState().getIndex().fileDetails().size(), is(0));
        }
    }

    public void testPeerRecoverySendSafeCommitInFileBased() throws Exception {
        IndexShard primaryShard = newStartedShard(true);
        int numDocs = between(1, 100);
        long globalCheckpoint = 0;
        for (int i = 0; i < numDocs; i++) {
            Engine.IndexResult result = primaryShard.applyIndexOperationOnPrimary(Versions.MATCH_ANY, VersionType.INTERNAL,
                                                                                  new SourceToParse(primaryShard.shardId().getIndexName(), "_doc",new BytesArray("{}"), XContentType.JSON),
                                                                                  SequenceNumbers.UNASSIGNED_SEQ_NO, 0, -1L, false);
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
            if (randomBoolean()) {
                globalCheckpoint = randomLongBetween(globalCheckpoint, i);
                primaryShard.updateLocalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.updateGlobalCheckpointForShard(primaryShard.routingEntry().allocationId().getId(), globalCheckpoint);
                primaryShard.flush(new FlushRequest());
            }
        }
        IndexShard replicaShard = newShard(primaryShard.shardId(), false);
        updateMappings(replicaShard, primaryShard.indexSettings().getIndexMetadata());
        recoverReplica(replicaShard, primaryShard, (r, sourceNode) -> new RecoveryTarget(r, sourceNode, recoveryListener) {
            @Override
            public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
                super.prepareForTranslogOperations(totalTranslogOps, listener);
                assertThat(replicaShard.getLastKnownGlobalCheckpoint(), equalTo(primaryShard.getLastKnownGlobalCheckpoint()));
            }
            @Override
            public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetaData,
                                   ActionListener<Void> listener) {
                assertThat(globalCheckpoint, equalTo(primaryShard.getLastKnownGlobalCheckpoint()));
                super.cleanFiles(totalTranslogOps, globalCheckpoint, sourceMetaData, listener);
            }
        }, true, true);
        List<IndexCommit> commits = DirectoryReader.listCommits(replicaShard.store().directory());
        long maxSeqNo = Long.parseLong(commits.get(0).getUserData().get(SequenceNumbers.MAX_SEQ_NO));
        assertThat(maxSeqNo, lessThanOrEqualTo(globalCheckpoint));
        closeShards(primaryShard, replicaShard);
    }
}
