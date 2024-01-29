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

package io.crate.fdw;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportCreateServerAction extends TransportMasterNodeAction<CreateServerRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();


    public static class Action extends ActionType<AcknowledgedResponse> {
        public static final String NAME = "internal:crate:sql/fdw/server/create";

        private Action() {
            super(NAME);
        }
    }

    private final ForeignDataWrappers foreignDataWrappers;


    @Inject
    public TransportCreateServerAction(TransportService transportService,
                                       ClusterService clusterService,
                                       ThreadPool threadPool,
                                       ForeignDataWrappers foreignDataWrappers) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            CreateServerRequest::new
        );
        this.foreignDataWrappers = foreignDataWrappers;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(CreateServerRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        if (state.nodes().getMinNodeVersion().before(Version.V_5_7_0)) {
            throw new IllegalStateException(
                "Cannot execute CREATE SERVER while there are <5.7.0 nodes in the cluster");
        }
        AddServerTask updateTask = new AddServerTask(foreignDataWrappers, request);
        updateTask.completionFuture().whenComplete(listener);
        clusterService.submitStateUpdateTask("create_server", updateTask);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateServerRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
