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

package io.crate.executor.transport;

import io.crate.Constants;
import io.crate.action.FutureActionListener;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CompletableFuture;

@Singleton
public class TransportSchemaUpdateAction extends TransportMasterNodeAction<SchemaUpdateRequest, SchemaUpdateResponse> {

    private final NodeClient nodeClient;

    @Inject
    public TransportSchemaUpdateAction(Settings settings,
                                       TransportService transportService,
                                       ClusterService clusterService,
                                       ThreadPool threadPool,
                                       ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver,
                                       NodeClient nodeClient) {
        super(settings,
            "crate/sql/ddl/schema_update",
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            indexNameExpressionResolver,
            SchemaUpdateRequest::new);
        this.nodeClient = nodeClient;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected SchemaUpdateResponse newResponse() {
        return new SchemaUpdateResponse(true);
    }

    @Override
    protected void masterOperation(SchemaUpdateRequest request, ClusterState state, ActionListener<SchemaUpdateResponse> listener) throws Exception {
        updateMapping(request.index(), request.masterNodeTimeout(), request.mappingSource())
            .thenApply(r -> new SchemaUpdateResponse(true))
            .whenComplete(ActionListeners.asBiConsumer(listener));
    }

    private CompletableFuture<PutMappingResponse> updateMapping(Index index,
                                                                TimeValue timeout,
                                                                String mappingSource) {
        FutureActionListener<PutMappingResponse, PutMappingResponse> putMappingListener = FutureActionListener.newInstance();
        PutMappingRequest putMappingRequest = new PutMappingRequest()
            .indices(new String[0])
            .setConcreteIndex(index)
            .type(Constants.DEFAULT_MAPPING_TYPE)
            .source(mappingSource)
            .timeout(timeout)
            .masterNodeTimeout(timeout);
        nodeClient.executeLocally(PutMappingAction.INSTANCE, putMappingRequest, putMappingListener);
        return putMappingListener;
    }

    @Override
    protected ClusterBlockException checkBlock(SchemaUpdateRequest request, ClusterState state) {
        return null;
    }
}
