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

package io.crate.es.action.admin.cluster.snapshots.create;

import io.crate.es.action.ActionListener;
import io.crate.es.action.support.master.TransportMasterNodeAction;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.settings.Settings;
import io.crate.es.snapshots.Snapshot;
import io.crate.es.snapshots.SnapshotInfo;
import io.crate.es.snapshots.SnapshotsService;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

/**
 * Transport action for create snapshot operation
 */
public class TransportCreateSnapshotAction extends TransportMasterNodeAction<CreateSnapshotRequest, CreateSnapshotResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportCreateSnapshotAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, SnapshotsService snapshotsService,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, CreateSnapshotAction.NAME, transportService, clusterService, threadPool, indexNameExpressionResolver, CreateSnapshotRequest::new);
        this.snapshotsService = snapshotsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    protected CreateSnapshotResponse newResponse() {
        return new CreateSnapshotResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(CreateSnapshotRequest request, ClusterState state) {
        // We are reading the cluster metadata and indices - so we need to check both blocks
        ClusterBlockException clusterBlockException = state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
        if (clusterBlockException != null) {
            return clusterBlockException;
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(final CreateSnapshotRequest request, ClusterState state, final ActionListener<CreateSnapshotResponse> listener) {
        final String snapshotName = indexNameExpressionResolver.resolveDateMathExpression(request.snapshot());
        SnapshotsService.SnapshotRequest snapshotRequest =
                new SnapshotsService.SnapshotRequest(request.repository(), snapshotName, "create_snapshot [" + snapshotName + "]")
                        .indices(request.indices())
                        .indicesOptions(request.indicesOptions())
                        .partial(request.partial())
                        .settings(request.settings())
                        .includeGlobalState(request.includeGlobalState())
                        .masterNodeTimeout(request.masterNodeTimeout());
        snapshotsService.createSnapshot(snapshotRequest, new SnapshotsService.CreateSnapshotListener() {
            @Override
            public void onResponse() {
                if (request.waitForCompletion()) {
                    snapshotsService.addListener(new SnapshotsService.SnapshotCompletionListener() {
                        @Override
                        public void onSnapshotCompletion(Snapshot snapshot, SnapshotInfo snapshotInfo) {
                            if (snapshot.getRepository().equals(request.repository()) &&
                                    snapshot.getSnapshotId().getName().equals(snapshotName)) {
                                listener.onResponse(new CreateSnapshotResponse(snapshotInfo));
                                snapshotsService.removeListener(this);
                            }
                        }

                        @Override
                        public void onSnapshotFailure(Snapshot snapshot, Exception e) {
                            if (snapshot.getRepository().equals(request.repository()) &&
                                    snapshot.getSnapshotId().getName().equals(snapshotName)) {
                                listener.onFailure(e);
                                snapshotsService.removeListener(this);
                            }
                        }
                    });
                } else {
                    listener.onResponse(new CreateSnapshotResponse());
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }
}
