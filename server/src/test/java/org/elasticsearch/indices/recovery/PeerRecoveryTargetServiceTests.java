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

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;

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
        List<StoreFileMetaData> mdFiles = new ArrayList<>();
        for (StoreFileMetaData md : sourceSnapshot) {
            mdFiles.add(md);
        }
        final IndexShard targetShard = newShard(false);
        final DiscoveryNode pNode = getFakeDiscoNode(sourceShard.routingEntry().currentNodeId());
        final DiscoveryNode rNode = getFakeDiscoNode(targetShard.routingEntry().currentNodeId());
        targetShard.markAsRecovering("test-peer-recovery", new RecoveryState(targetShard.routingEntry(), rNode, pNode));
        final RecoveryTarget recoveryTarget = new RecoveryTarget(targetShard, null, null, null);
        recoveryTarget.receiveFileInfo(
            mdFiles.stream().map(StoreFileMetaData::name).collect(Collectors.toList()),
            mdFiles.stream().map(StoreFileMetaData::length).collect(Collectors.toList()),
            Collections.emptyList(), Collections.emptyList(), 0
        );
        List<RecoveryFileChunkRequest> requests = new ArrayList<>();
        for (StoreFileMetaData md : mdFiles) {
            try (IndexInput in = sourceShard.store().directory().openInput(md.name(), IOContext.READONCE)) {
                int pos = 0;
                while (pos < md.length()) {
                    int length = between(1, Math.toIntExact(md.length() - pos));
                    byte[] buffer = new byte[length];
                    in.readBytes(buffer, 0, length);
                    requests.add(new RecoveryFileChunkRequest(
                        0, sourceShard.shardId(), md, pos, new BytesArray(buffer),
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
        recoveryTarget.renameAllTempFiles();
        recoveryTarget.decRef();
        Store.MetadataSnapshot targetSnapshot = targetShard.snapshotStoreMetadata();
        Store.RecoveryDiff diff = sourceSnapshot.recoveryDiff(targetSnapshot);
        assertThat(diff.different, empty());
        closeShards(sourceShard, targetShard);
    }
}
