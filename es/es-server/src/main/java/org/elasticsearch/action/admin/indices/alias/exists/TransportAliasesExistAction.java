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
package org.elasticsearch.action.admin.indices.alias.exists;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportAliasesExistAction extends TransportMasterNodeReadAction<GetAliasesRequest, AliasesExistResponse> {

    @Inject
    public TransportAliasesExistAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, AliasesExistAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, GetAliasesRequest::new);
    }

    @Override
    protected String executor() {
        // very lightweight operation, no need to fork
        return ThreadPool.Names.SAME;
    }

    @Override
    protected ClusterBlockException checkBlock(GetAliasesRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    @Override
    protected AliasesExistResponse newResponse() {
        return new AliasesExistResponse();
    }

    @Override
    protected void masterOperation(GetAliasesRequest request, ClusterState state, ActionListener<AliasesExistResponse> listener) {
        String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        boolean result = state.metaData().hasAliases(request.aliases(), concreteIndices);
        listener.onResponse(new AliasesExistResponse(result));
    }

}
