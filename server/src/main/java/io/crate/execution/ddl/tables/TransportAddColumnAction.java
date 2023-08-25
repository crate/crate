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

import io.crate.metadata.Reference;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.metadata.NodeContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

@Singleton
public class TransportAddColumnAction extends TransportMasterNodeAction<AddColumnRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/table/add_column";
    private final NodeContext nodeContext;
    private final IndicesService indicesService;
    private final Writeable.Reader<AcknowledgedResponse> reader;
    private final BiFunction<Boolean, List<Reference>, AcknowledgedResponse> ackedResponseFunction;

    @Inject
    public TransportAddColumnAction(TransportService transportService,
                                    ClusterService clusterService,
                                    IndicesService indicesService,
                                    ThreadPool threadPool,
                                    NodeContext nodeContext) {
        super(
            ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            AddColumnRequest::new
        );
        if (clusterService.state().nodes().getMinNodeVersion().onOrAfter(Version.V_5_5_0)) {
            reader = AddColumnResponse::new;
            ackedResponseFunction = AddColumnResponse::new;
        } else {
            reader = AcknowledgedResponse::new;
            ackedResponseFunction = (acked, ignored) -> new AcknowledgedResponse(acked);
        }
        this.nodeContext = nodeContext;
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return reader.read(in);
    }

    @Override
    protected void masterOperation(AddColumnRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) throws Exception {
        AddColumnTask addColumnTask = new AddColumnTask(nodeContext, indicesService::createIndexMapperService);

        clusterService.submitStateUpdateTask("add-column",
            new AckedClusterStateUpdateTask<>(Priority.HIGH, request, listener) {

                private List<Reference> addedColumns = new ArrayList<>();

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState updatedState = addColumnTask.execute(currentState, request);
                    addedColumns = addColumnTask.addedColumns();
                    return updatedState;
                }

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return ackedResponseFunction.apply(acknowledged, addedColumns);
                }
            });
    }

    @Override
    public ClusterBlockException checkBlock(AddColumnRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
