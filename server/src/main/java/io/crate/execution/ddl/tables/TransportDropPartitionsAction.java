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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.function.Function;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MetadataDeleteIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;

@Singleton
public class TransportDropPartitionsAction extends AbstractDDLTransportAction<DropPartitionsRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();

    public static class Action extends ActionType<AcknowledgedResponse> {
        public static final String NAME = "internal:crate:sql/table/drop-partitions";

        private Action() {
            super(NAME);
        }
    }

    private final DDLClusterStateTaskExecutor<DropPartitionsRequest> executor;

    @Inject
    public TransportDropPartitionsAction(TransportService transportService,
                                         ClusterService clusterService,
                                         ThreadPool threadPool,
                                         MetadataDeleteIndexService deleteIndexService) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            DropPartitionsRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "drop-table-partitions"
        );
        executor = new DDLClusterStateTaskExecutor<>() {
            @Override
            protected ClusterState execute(ClusterState currentState, DropPartitionsRequest request) {
                Collection<Index> indices = getIndices(currentState, request, IndexMetadata::getIndex);

                if (indices.isEmpty()) {
                    return currentState;
                } else {
                    return deleteIndexService.deleteIndices(currentState, indices);
                }
            }
        };
    }

    private static <T> Collection<T> getIndices(ClusterState currentState,
                                                DropPartitionsRequest request,
                                                Function<IndexMetadata, T> mapFunction) {
        Collection<T> indices;
        if (request.partitionValues().isEmpty()) {
            indices = currentState.metadata().getIndices(
                request.relationName(),
                List.of(),
                false,
                mapFunction);
        } else {
            indices = new HashSet<>();
            for (List<String> values : request.partitionValues()) {
                indices.addAll(currentState.metadata().getIndices(
                    request.relationName(),
                    values,
                    false,
                    mapFunction)
                );
            }
        }
        return indices;
    }

    @Override
    public ClusterStateTaskExecutor<DropPartitionsRequest> clusterStateTaskExecutor(DropPartitionsRequest request) {
        return executor;
    }

    @Override
    protected ClusterBlockException checkBlock(DropPartitionsRequest request, ClusterState state) {
        String[] indexNames = getIndices(state, request, im -> im.getIndex().getName()).toArray(String[]::new);
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, indexNames);
    }
}
