/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.replication.logical.repository;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;

import io.crate.common.collections.Sets;
import io.crate.common.io.IOUtils;
import io.crate.replication.logical.action.RestoreShardRequest;
import io.crate.replication.logical.seqno.RetentionLeaseHelper;

/*
 * Restore source service tracks all the ongoing restore operations
 * relying on the leader shards. Once the restore is completed the
 * relevant resources are released. Also, listens on the index events
 * to update the resources
 *
 * Derived from org.opensearch.replication.repository.RemoteClusterRestoreLeaderService
 */
public class PublisherRestoreService extends AbstractLifecycleComponent {

    private final IndicesService indicesService;
    private final NodeClient nodeClient;
    private final Map<String, RestoreContext> onGoingRestores = new ConcurrentHashMap<>();
    private final Set<Closeable> closableResources = Sets.newConcurrentHashSet();

    @Inject
    public PublisherRestoreService(IndicesService indicesService, NodeClient nodeClient) {
        this.indicesService = indicesService;
        this.nodeClient = nodeClient;
    }

    @Override
    protected void doStart() {

    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() throws IOException {
        IOUtils.close(closableResources);
    }

    public void createRestoreContext(String restoreUUID,
                                     RestoreShardRequest request,
                                     Consumer<RestoreContext> onSuccess,
                                     Consumer<Exception> onFailure) {
        constructRestoreContext(
            restoreUUID,
            request,
            context -> {
                onGoingRestores.putIfAbsent(restoreUUID, context);
                onSuccess.accept(context);
            },
            onFailure
        );
    }

    private RestoreContext getRestoreContextSafe(String restoreUUID) {
        var restoreContext = onGoingRestores.get(restoreUUID);
        if (restoreContext == null) {
            throw new IllegalStateException("missing restoreContext");
        }
        return restoreContext;
    }

    public InputStreamIndexInput openInputStream(String restoreUUID,
                                                 RestoreShardRequest request,
                                                 String fileName,
                                                 long length) {
        var leaderIndexShard = indicesService.getShardOrNull(request.shardId());
        if (leaderIndexShard == null) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Shard [%s] missing", request.shardId()));
        }
        var restoreContext = getRestoreContextSafe(restoreUUID);
        var indexInput = restoreContext.openInput(fileName);

        return new InputStreamIndexInput(indexInput, length) {
            @Override
            public void close() throws IOException {
                super.close();
                IOUtils.close(indexInput);
            }
        };
    }

    private void constructRestoreContext(String restoreUUID,
                                         RestoreShardRequest request,
                                         Consumer<RestoreContext> onSuccess,
                                         Consumer<Exception> onFailure) {
        var leaderIndexShard = indicesService.getShardOrNull(request.shardId());
        if (leaderIndexShard == null) {
            onFailure.accept(new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Shard [%s] missing", request.shardId())));
            return;
        }
        /**
         * ODFE Replication supported for >= CrateDB 4.5. History of operations directly from
         * lucene index. With the retention lock set - safe commit should have all the history
         * upto the current retention leases.
         */
        var retentionLock = leaderIndexShard.acquireHistoryRetentionLock(Engine.HistorySource.INDEX);
        closableResources.add(retentionLock);

        /**
         * Construct restore via safe index commit
         * at the leader cluster. All the references from this commit
         * should be available until it is closed.
         */
        var indexCommitRef = leaderIndexShard.acquireSafeIndexCommit();

        var store = leaderIndexShard.store();
        var metadataSnapshot = Store.MetadataSnapshot.EMPTY;

        store.incRef();
        try {
            metadataSnapshot = store.getMetadata(indexCommitRef.getIndexCommit());
        } catch (IOException e) {
            if (closeRetentionLock(retentionLock, onFailure)) {
                onFailure.accept(new UncheckedIOException(e));
            }
            return;
        } finally {
            store.decRef();
        }

        // Identifies the seq no to start the replication operations from
        var fromSeqNo = RetentionLeaseActions.RETAIN_ALL;

        final var finalMetadataSnapshot = metadataSnapshot;
        // Adds the retention lease for fromSeqNo for the next stage of the replication.
        RetentionLeaseHelper.addRetentionLease(
            request.shardId(),
            fromSeqNo,
            request.subscriberClusterName(),
            nodeClient,
            ActionListener.wrap(
                r -> {
                    /**
                     * At this point, it should be safe to release retention lock as the retention lease
                     * is acquired from the local checkpoint and the rest of the follower replay actions
                     * can be performed using this retention lease.
                     */
                    if (closeRetentionLock(retentionLock, onFailure)) {
                        var restoreContext = new RestoreContext(indexCommitRef, finalMetadataSnapshot);
                        onGoingRestores.put(restoreUUID, restoreContext);

                        closableResources.add(restoreContext);
                        onSuccess.accept(restoreContext);
                    }
                },
                e -> {
                    if (closeRetentionLock(retentionLock, onFailure)) {
                        onFailure.accept(e);
                    }
                }
            )
        );
    }

    private boolean closeRetentionLock(Closeable retentionLock, Consumer<Exception> onFailure) {
        try {
            retentionLock.close();
            return true;
        } catch (IOException e) {
            onFailure.accept(new UncheckedIOException(e));
            return false;
        }
    }

    public void removeRestoreContext(String restoreUUID) {
        var restoreContext = onGoingRestores.remove(restoreUUID);
        if (restoreContext != null) {
            restoreContext.close();
        }
    }


    public static class RestoreContext implements Closeable {

        private final Engine.IndexCommitRef indexCommitRef;
        private final Store.MetadataSnapshot metadataSnapshot;

        public RestoreContext(Engine.IndexCommitRef indexCommitRef,
                              Store.MetadataSnapshot metadataSnapshot) {
            this.indexCommitRef = indexCommitRef;
            this.metadataSnapshot = metadataSnapshot;
        }

        public IndexInput openInput(String fileName) {
            IndexInput currentIndexInput;
            try {
                currentIndexInput = indexCommitRef.getIndexCommit().getDirectory().openInput(fileName,
                                                                                             IOContext.READONCE);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return currentIndexInput;
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(indexCommitRef);
        }

        public Store.MetadataSnapshot metadataSnapshot() {
            return metadataSnapshot;
        }
    }
}
