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

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;


public class TransportCreateBlobTableAction extends TransportMasterNodeAction<CreateBlobTableRequest, CreateTableResponse> {

    public static final Action ACTION = new Action();
    private final MetadataCreateIndexService createIndexService;

    public static class Action extends ActionType<CreateTableResponse> {

        public static final String NAME = "internal:crate:sql/tables/blob/create";

        public Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportCreateBlobTableAction(TransportService transportService,
                                          ClusterService clusterService,
                                          ThreadPool threadPool,
                                          MetadataCreateIndexService createIndexService) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            CreateBlobTableRequest::new
        );
        this.createIndexService = createIndexService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected CreateTableResponse read(StreamInput in) throws IOException {
        return new CreateTableResponse(in);
    }

    @Override
    protected void masterOperation(CreateBlobTableRequest request,
                                   ClusterState state,
                                   ActionListener<CreateTableResponse> listener) throws Exception {
        createIndexService.addBlobTable(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CreateBlobTableRequest request, ClusterState state) {
        String indexName = request.name().indexNameOrAlias();
        return state.blocks().indexBlockedException(ClusterBlockLevel.METADATA_WRITE, indexName);
    }
}
