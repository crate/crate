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

package org.elasticsearch.action.admin.cluster.snapshots.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport action for create snapshot operation
 */
public class TransportCreateSnapshotAction extends TransportMasterNodeAction<CreateSnapshotRequest, CreateSnapshotResponse> {
    private final SnapshotsService snapshotsService;

    @Inject
    public TransportCreateSnapshotAction(TransportService transportService, ClusterService clusterService,
                                         ThreadPool threadPool, SnapshotsService snapshotsService,
                                         IndexNameExpressionResolver indexNameExpressionResolver) {
        super(CreateSnapshotAction.NAME, transportService, clusterService, threadPool,
            CreateSnapshotRequest::new, indexNameExpressionResolver);
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
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected void masterOperation(final CreateSnapshotRequest request, ClusterState state,
                                   final ActionListener<CreateSnapshotResponse> listener) {
        if (request.waitForCompletion()) {
            snapshotsService.executeSnapshot(request, ActionListener.map(listener, CreateSnapshotResponse::new));
        } else {
            snapshotsService.createSnapshot(request, ActionListener.map(listener, snapshot -> new CreateSnapshotResponse()));
        }
    }
}
