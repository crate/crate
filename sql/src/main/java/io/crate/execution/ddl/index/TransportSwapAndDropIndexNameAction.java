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

package io.crate.execution.ddl.index;

import io.crate.execution.ddl.AbstractDDLTransportAction;
import io.crate.metadata.cluster.SwapAndDropIndexExecutor;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.cluster.ClusterState;
import io.crate.es.cluster.ClusterStateTaskExecutor;
import io.crate.es.cluster.block.ClusterBlockException;
import io.crate.es.cluster.block.ClusterBlockLevel;
import io.crate.es.cluster.metadata.IndexNameExpressionResolver;
import io.crate.es.cluster.routing.allocation.AllocationService;
import io.crate.es.cluster.service.ClusterService;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.inject.Singleton;
import io.crate.es.common.settings.Settings;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

/**
 * Renames a sourceIndex to targetIndex, and drops the former targetIndex - effectively overriding the target
 */
@Singleton
public class TransportSwapAndDropIndexNameAction extends AbstractDDLTransportAction<SwapAndDropIndexRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/index/swap_and_drop_index";

    private final SwapAndDropIndexExecutor executor;

    @Inject
    public TransportSwapAndDropIndexNameAction(Settings settings,
                                               TransportService transportService,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               AllocationService allocationService,
                                               IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, threadPool,
            indexNameExpressionResolver, SwapAndDropIndexRequest::new, AcknowledgedResponse::new, AcknowledgedResponse::new,
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
