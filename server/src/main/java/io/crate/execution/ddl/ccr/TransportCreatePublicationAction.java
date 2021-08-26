/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.execution.ddl.ccr;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportCreatePublicationAction extends AbstractDDLTransportAction<CreatePublicationRequest, AcknowledgedResponse> {

    public static final String ACTION_NAME = "crate:ccr/publication/create";

    private final ClusterStateTaskExecutor<CreatePublicationRequest> executor;

    public TransportCreatePublicationAction(TransportService transportService,
                                            ClusterService clusterService,
                                            ThreadPool threadPool,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME,
              transportService,
              clusterService,
              threadPool,
              indexNameExpressionResolver,
              CreatePublicationRequest::new,
              AcknowledgedResponse::new,
              AcknowledgedResponse::new,
              "create-publication");
        this.executor = new CreatePublicationStateExecutor();
    }

    @Override
    public ClusterStateTaskExecutor<CreatePublicationRequest> clusterStateTaskExecutor(CreatePublicationRequest request) {
        return executor;
    }

    @Override
    protected ClusterBlockException checkBlock(CreatePublicationRequest request,
                                               ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
