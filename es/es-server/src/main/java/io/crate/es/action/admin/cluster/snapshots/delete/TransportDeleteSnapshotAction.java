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

package io.crate.es.action.admin.cluster.snapshots.delete;

import io.crate.es.action.ActionListener;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.action.support.master.TransportMasterNodeAction;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.settings.Settings;
import io.crate.es.snapshots.SnapshotsService;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

/**
 * Transport action for delete snapshot operation
 */
public class TransportDeleteSnapshotAction extends TransportMasterNodeAction<DeleteSnapshotRequest, AcknowledgedResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportDeleteSnapshotAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, SnapshotsService snapshotsService,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, DeleteSnapshotAction.NAME, transportService, clusterService, threadPool, indexNameExpressionResolver, DeleteSnapshotRequest::new);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteSnapshotRequest request, ClusterState state) {
        // Cluster is not affected but we look up repositories in metadata
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected void masterOperation(final DeleteSnapshotRequest request, ClusterState state, final ActionListener<AcknowledgedResponse> listener) {
        snapshotsService.deleteSnapshot(request.repository(), request.snapshot(), new SnapshotsService.DeleteSnapshotListener() {
            @Override
            public void onResponse() {
                listener.onResponse(new AcknowledgedResponse(true));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        }, false);
    }
}
