/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.ddl.tables;

import java.util.HashSet;
import java.util.Set;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;

@Singleton
public class TransportGCDanglingArtifacts extends AbstractDDLTransportAction<GCDanglingArtifactsRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "internal:crate:admin/gc";

        private Action() {
            super(NAME);
        }
    }

    private final DDLClusterStateTaskExecutor<GCDanglingArtifactsRequest> executor;

    @Inject
    public TransportGCDanglingArtifacts(TransportService transportService,
                                        ClusterService clusterService,
                                        ThreadPool threadPool,
                                        MetadataDeleteIndexService deleteIndexService) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            GCDanglingArtifactsRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "gc"
        );
        executor = new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState,
                                           GCDanglingArtifactsRequest gcDanglingArtifactsRequest) {
                Metadata metadata = currentState.metadata();
                Set<Index> danglingIndicesToDelete = new HashSet<>();
                for (ObjectCursor<IndexMetadata> indexMetadata : metadata.indices().values()) {
                    Index index = indexMetadata.value.getIndex();
                    RelationMetadata relation = metadata.getRelation(index.getUUID());
                    if (relation == null) {
                        danglingIndicesToDelete.add(index);
                    }
                }
                if (danglingIndicesToDelete.isEmpty()) {
                    return currentState;
                }
                return deleteIndexService.deleteIndices(currentState, danglingIndicesToDelete);
            }
        };
    }

    @Override
    public ClusterStateTaskExecutor<GCDanglingArtifactsRequest> clusterStateTaskExecutor(GCDanglingArtifactsRequest request) {
        return executor;
    }

    @Override
    protected ClusterBlockException checkBlock(GCDanglingArtifactsRequest request, ClusterState state) {
        return null;
    }
}
