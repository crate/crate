/*
 * Licensed to Crate.io Inc. ("Crate.io") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate.io licenses
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
 * To enable or use any of the enterprise features, Crate.io must have given
 * you permission to enable and use the Enterprise Edition of CrateDB and you
 * must have a valid Enterprise or Subscription Agreement with Crate.io.  If
 * you enable or use features that are part of the Enterprise Edition, you
 * represent and warrant that you have a valid Enterprise or Subscription
 * Agreement with Crate.io.  Your use of features of the Enterprise Edition
 * is governed by the terms and conditions of your Enterprise or Subscription
 * Agreement with Crate.io.
 */

package io.crate.expression.udf;

import io.crate.exceptions.ScriptException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportCreateUserDefinedFunctionAction
    extends TransportMasterNodeAction<CreateUserDefinedFunctionRequest, UserDefinedFunctionResponse> {

    private final UserDefinedFunctionService udfService;

    @Inject
    public TransportCreateUserDefinedFunctionAction(Settings settings,
                                                    TransportService transportService,
                                                    ClusterService clusterService,
                                                    ThreadPool threadPool,
                                                    UserDefinedFunctionService udfService,
                                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, "internal:crate:sql/udf/create", transportService, clusterService, threadPool,
            indexNameExpressionResolver, CreateUserDefinedFunctionRequest::new);
        this.udfService = udfService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected UserDefinedFunctionResponse newResponse() {
        return new UserDefinedFunctionResponse();
    }

    @Override
    protected void masterOperation(final CreateUserDefinedFunctionRequest request,
                                   ClusterState state,
                                   ActionListener<UserDefinedFunctionResponse> listener) throws Exception {

        UserDefinedFunctionMetaData metaData = request.userDefinedFunctionMetaData();
        String errorMessage = udfService.getLanguage(metaData.language()).validate(metaData);
        if (errorMessage != null) {
            throw new ScriptException(errorMessage, metaData.language());
        }

        udfService.registerFunction(metaData, request.replace(), listener, request.masterNodeTimeout());
    }

    @Override
    protected ClusterBlockException checkBlock(CreateUserDefinedFunctionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
