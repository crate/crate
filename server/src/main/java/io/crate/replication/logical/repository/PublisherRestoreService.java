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

import io.crate.common.io.IOUtils;
import io.crate.replication.logical.action.RestoreShardRequest;
import io.crate.replication.logical.seqno.RetentionLeaseHelper;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.RetentionLeaseActions;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
    private final Map<String, RestoreContext> onGoingRestores = ConcurrentCollections.newConcurrentMap();
    private final Set<Closeable> closableResources = ConcurrentCollections.newConcurrentSet();

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

    public <T extends RestoreShardRequest<T>> RestoreContext createRestoreContext(String restoreUUID,
                                                                                  RestoreShardRequest<T> request) {
        return onGoingRestores.putIfAbsent(restoreUUID, constructRestoreContext(restoreUUID, request));
    }

    private RestoreContext getRestoreContext(String restoreUUID) {
        var restoreContext = onGoingRestores.get(restoreUUID);
        if (restoreContext == null) {
            throw new IllegalStateException("missing restoreContext");
        }
        return restoreContext;
    }

    public <T extends RestoreShardRequest<T>> InputStreamIndexInput openInputStream(String restoreUUID,
                                                                                    RestoreShardRequest<T> request,
                                                                                    String fileName,
                                                                                    long length) {
        var leaderIndexShard = indicesService.getShardOrNull(request.publisherShardId());
        if (leaderIndexShard == null) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Shard [%s] missing", request.publisherShardId()));
        }
        var store = leaderIndexShard.store();
        var restoreContext = getRestoreContext(restoreUUID);
        var indexInput = restoreContext.openInput(store, fileName);

        return new InputStreamIndexInput(indexInput, length) {
            @Override
            public void close() throws IOException {
                super.close();
                IOUtils.close(indexInput);
            }
        };
    }

    public <T extends RestoreShardRequest<T>> RestoreContext constructRestoreContext(String restoreUUID,
                                                                                     RestoreShardRequest<T> request) {
        var leaderIndexShard = indicesService.getShardOrNull(request.publisherShardId());
        if (leaderIndexShard == null) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Shard [%s] missing", request.publisherShardId()));
        }
        /**
         * ODFE Replication supported for >= ES 7.8. History of operations directly from
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
            throw new UncheckedIOException(e);
        } finally {
            store.decRef();
        }

        // Identifies the seq no to start the replication operations from
        var fromSeqNo = RetentionLeaseActions.RETAIN_ALL;

        // Passing nodeclient of the leader to acquire the retention lease on leader shard
        var retentionLeaseHelper = new RetentionLeaseHelper(request.subscriberClusterName(), nodeClient);
        // Adds the retention lease for fromSeqNo for the next stage of the replication.
        retentionLeaseHelper.addRetentionLease(
            request.publisherShardId(),
            fromSeqNo,
            request.subscriberShardId(),
            LogicalReplicationRepository.REMOTE_CLUSTER_REPO_REQ_TIMEOUT_IN_MILLI_SEC
        );

        /**
         * At this point, it should be safe to release retention lock as the retention lease
         * is acquired from the local checkpoint and the rest of the follower replay actions
         * can be performed using this retention lease.
         */
        try {
            retentionLock.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        var restoreContext = new RestoreContext(indexCommitRef, metadataSnapshot);
        onGoingRestores.put(restoreUUID, restoreContext);

        closableResources.add(restoreContext);
        return restoreContext;
    }

    public void removeRestoreContext(String restoreUUID) {
        var restoreContext = onGoingRestores.remove(restoreUUID);
        if (restoreContext != null) {
            restoreContext.close();
        }
    }


    public static class RestoreContext implements Closeable {

        private static final int INITIAL_FILE_CACHE_CAPACITY = 20;

        private final Engine.IndexCommitRef indexCommitRef;
        private final Store.MetadataSnapshot metadataSnapshot;
        private final LinkedHashMap<String, IndexInput> currentFiles = new LinkedHashMap<>(INITIAL_FILE_CACHE_CAPACITY);

        public RestoreContext(Engine.IndexCommitRef indexCommitRef,
                              Store.MetadataSnapshot metadataSnapshot) {
            this.indexCommitRef = indexCommitRef;
            this.metadataSnapshot = metadataSnapshot;
        }

        public IndexInput openInput(Store store, String fileName) {
            var currentIndexInput = currentFiles.getOrDefault(fileName, null);
            if (currentIndexInput != null) {
                return currentIndexInput.clone();
            }

            store.incRef();
            try {
                currentIndexInput = store.directory().openInput(fileName, IOContext.READONCE);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                store.decRef();
            }

            currentFiles.put(fileName, currentIndexInput);
            return currentIndexInput.clone();
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(currentFiles.values());
            IOUtils.closeWhileHandlingException(indexCommitRef);
        }

        public Store.MetadataSnapshot metadataSnapshot() {
            return metadataSnapshot;
        }
    }
}
