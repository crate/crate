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

package io.crate.operation.udf;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportCreateUserDefinedFunctionAction extends TransportMasterNodeAction<CreateUserDefinedFunctionRequest, CreateUserDefinedFunctionResponse> {

    private final UserDefinedFunctionService userDefinedFunctionService;

    @Inject
    public TransportCreateUserDefinedFunctionAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                                    ThreadPool threadPool, UserDefinedFunctionService userDefinedFunctionService,
                                                    ActionFilters actionFilters,
                                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, "createuserdefinedfunction", transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, CreateUserDefinedFunctionRequest.class);
        this.userDefinedFunctionService = userDefinedFunctionService;

    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected CreateUserDefinedFunctionResponse newResponse() {
        return new CreateUserDefinedFunctionResponse();
    }

    @Override
    protected void masterOperation(final CreateUserDefinedFunctionRequest request,
                                   ClusterState state, ActionListener<CreateUserDefinedFunctionResponse> listener) throws Exception {
        userDefinedFunctionService.registerFunction(
            new UserDefinedFunctionService.RegisterUserDefinedFunctionRequest(
                "put_udf [" + request.userDefinedFunctionMetaData().name() + "]",
                request.userDefinedFunctionMetaData().name(),
                request.userDefinedFunctionMetaData(),
                request.replace()
            ).masterNodeTimeout(request.masterNodeTimeout()),
            new ActionListener<ClusterStateUpdateResponse>() {
                @Override
                public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                    listener.onResponse(new CreateUserDefinedFunctionResponse(clusterStateUpdateResponse.isAcknowledged()));
                }

                @Override
                public void onFailure(Throwable e) {
                    listener.onFailure(e);
                }
            }
        );

    }

    @Override
    protected ClusterBlockException checkBlock(CreateUserDefinedFunctionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
