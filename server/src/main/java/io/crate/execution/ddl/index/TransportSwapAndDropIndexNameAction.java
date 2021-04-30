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

package io.crate.execution.ddl.index;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.SwapAndDropIndexExecutor;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Renames a sourceIndex to targetIndex, and drops the former targetIndex - effectively overriding the target
 */
@Singleton
public class TransportSwapAndDropIndexNameAction extends AbstractDDLTransportAction<SwapAndDropIndexRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/index/swap_and_drop_index";

    private final SwapAndDropIndexExecutor executor;

    @Inject
    public TransportSwapAndDropIndexNameAction(TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               AllocationService allocationService,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(ACTION_NAME,
            transportService,
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            SwapAndDropIndexRequest::new,
            AcknowledgedResponse::new,
            AcknowledgedResponse::new,
            "swap-and-drop-index");
        executor = new SwapAndDropIndexExecutor(allocationService);
    }

    @Override
    public ClusterStateTaskExecutor<SwapAndDropIndexRequest> clusterStateTaskExecutor(SwapAndDropIndexRequest request) {
        return executor;
    }

    @Override
    protected ClusterBlockException checkBlock(SwapAndDropIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(
            ClusterBlockLevel.METADATA_WRITE,
            new String[] { request.source(), request.target() });
    }
}
