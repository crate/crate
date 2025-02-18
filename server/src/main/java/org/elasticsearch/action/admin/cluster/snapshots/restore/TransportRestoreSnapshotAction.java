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

import java.io.IOException;

import org.apache.logging.log4j.util.Strings;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.RestoreService.RestoreCompletionResponse;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for restore snapshot operation
 */
public class TransportRestoreSnapshotAction extends TransportMasterNodeAction<RestoreSnapshotRequest, RestoreSnapshotResponse> {
    private final RestoreService restoreService;

    @Inject
    public TransportRestoreSnapshotAction(TransportService transportService,
                                          ClusterService clusterService,
                                          ThreadPool threadPool,
                                          RestoreService restoreService) {
        super(RestoreSnapshotAction.NAME, transportService, clusterService, threadPool, RestoreSnapshotRequest::new);
        this.restoreService = restoreService;
    }

    @Override
    protected String executor() {
        // Using the generic instead of the snapshot threadpool here as the snapshot threadpool might be blocked on long running tasks
        // which would block the request from getting an error response because of the ongoing task
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected RestoreSnapshotResponse read(StreamInput in) throws IOException {
        return new RestoreSnapshotResponse(in);
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
    protected void masterOperation(final RestoreSnapshotRequest request,
                                   final ClusterState state,
                                   final ActionListener<RestoreSnapshotResponse> listener) {
        RestoreService.RestoreRequest restoreRequest = new RestoreService.RestoreRequest(
            request.repository(),
            request.snapshot(),
            request.indicesOptions(),
            request.settings(),
            request.masterNodeTimeout(),
            false,
            request.includeAliases(),
            Settings.EMPTY,
            Strings.EMPTY_ARRAY,
            request.includeIndices(),
            request.includeCustomMetadata(),
            request.customMetadataTypes(),
            request.includeGlobalSettings(),
            request.globalSettings()
        );
        restoreService.restoreSnapshot(restoreRequest, request.tablesToRestore(), new ActionListener<>() {
            @Override
            public void onResponse(RestoreCompletionResponse restoreCompletionResponse) {
                if (restoreCompletionResponse.getRestoreInfo() == null && request.waitForCompletion()) {
                    clusterService.addListener(new RestoreClusterStateListener(clusterService, restoreCompletionResponse, listener));
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
