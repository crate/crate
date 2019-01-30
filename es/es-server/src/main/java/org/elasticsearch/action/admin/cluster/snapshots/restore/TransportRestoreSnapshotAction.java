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

package org.elasticsearch.action.admin.cluster.snapshots.restore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.RestoreService.RestoreCompletionResponse;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import static org.elasticsearch.snapshots.RestoreService.restoreInProgress;

/**
 * Transport action for restore snapshot operation
 */
public class TransportRestoreSnapshotAction extends TransportMasterNodeAction<RestoreSnapshotRequest, RestoreSnapshotResponse> {
    private final RestoreService restoreService;

    @Inject
    public TransportRestoreSnapshotAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, RestoreService restoreService, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, RestoreSnapshotAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, RestoreSnapshotRequest::new);
        this.restoreService = restoreService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    protected RestoreSnapshotResponse newResponse() {
        return new RestoreSnapshotResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(RestoreSnapshotRequest request, ClusterState state) {
        // Restoring a snapshot might change the global state and create/change an index,
        // so we need to check for METADATA_WRITE and WRITE blocks
        ClusterBlockException blockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
        if (blockException != null) {
            return blockException;
        }
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);

    }

    @Override
    protected void masterOperation(final RestoreSnapshotRequest request, final ClusterState state, final ActionListener<RestoreSnapshotResponse> listener) {
        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(request.repository(), request.snapshot(),
                request.indices(), request.templates(), request.indicesOptions(), request.renamePattern(), request.renameReplacement(),
                request.settings(), request.masterNodeTimeout(), request.includeGlobalState(), request.partial(), request.includeAliases(),
                request.indexSettings(), request.ignoreIndexSettings(), "restore_snapshot[" + request.snapshot() + "]");

        restoreService.restoreSnapshot(restoreRequest, new ActionListener<RestoreCompletionResponse>() {
            @Override
            public void onResponse(RestoreCompletionResponse restoreCompletionResponse) {
                if (restoreCompletionResponse.getRestoreInfo() == null && request.waitForCompletion()) {
                    final Snapshot snapshot = restoreCompletionResponse.getSnapshot();

                    ClusterStateListener clusterStateListener = new ClusterStateListener() {
                        @Override
                        public void clusterChanged(ClusterChangedEvent changedEvent) {
                            final RestoreInProgress.Entry prevEntry = restoreInProgress(changedEvent.previousState(), snapshot);
                            final RestoreInProgress.Entry newEntry = restoreInProgress(changedEvent.state(), snapshot);
                            if (prevEntry == null) {
                                // When there is a master failure after a restore has been started, this listener might not be registered
                                // on the current master and as such it might miss some intermediary cluster states due to batching.
                                // Clean up listener in that case and acknowledge completion of restore operation to client.
                                clusterService.removeListener(this);
                                listener.onResponse(new RestoreSnapshotResponse(null));
                            } else if (newEntry == null) {
                                clusterService.removeListener(this);
                                ImmutableOpenMap<ShardId, RestoreInProgress.ShardRestoreStatus> shards = prevEntry.shards();
                                assert prevEntry.state().completed() : "expected completed snapshot state but was " + prevEntry.state();
                                assert RestoreService.completed(shards) : "expected all restore entries to be completed";
                                RestoreInfo ri = new RestoreInfo(prevEntry.snapshot().getSnapshotId().getName(),
                                    prevEntry.indices(),
                                    shards.size(),
                                    shards.size() - RestoreService.failedShards(shards));
                                RestoreSnapshotResponse response = new RestoreSnapshotResponse(ri);
                                logger.debug("restore of [{}] completed", snapshot);
                                listener.onResponse(response);
                            } else {
                                // restore not completed yet, wait for next cluster state update
                            }
                        }
                    };

                    clusterService.addListener(clusterStateListener);
                } else {
                    listener.onResponse(new RestoreSnapshotResponse(restoreCompletionResponse.getRestoreInfo()));
                }
            }

            @Override
            public void onFailure(Exception t) {
                listener.onFailure(t);
            }
        });
    }
}
