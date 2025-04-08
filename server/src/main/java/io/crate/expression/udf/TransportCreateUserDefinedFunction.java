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

package io.crate.expression.udf;

import java.io.IOException;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import io.crate.exceptions.ScriptException;

public class TransportCreateUserDefinedFunction
    extends TransportMasterNodeAction<CreateUserDefinedFunctionRequest, AcknowledgedResponse> {

    public static final Action ACTION = new Action();
    private final UserDefinedFunctionService udfService;

    public static class Action extends ActionType<AcknowledgedResponse> {
        private static final String NAME = "internal:crate:sql/udf/create";

        private Action() {
            super(NAME);
        }
    }

    @Inject
    public TransportCreateUserDefinedFunction(TransportService transportService,
                                              ClusterService clusterService,
                                              ThreadPool threadPool,
                                              UserDefinedFunctionService udfService) {
        super(
            ACTION.name(),
            transportService,
            clusterService,
            threadPool,
            CreateUserDefinedFunctionRequest::new
        );
        this.udfService = udfService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(final CreateUserDefinedFunctionRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        UserDefinedFunctionMetadata metadata = request.userDefinedFunctionMetadata();
        String errorMessage = udfService.getLanguage(metadata.language()).validate(metadata);
        if (errorMessage != null) {
            throw new ScriptException(errorMessage, metadata.language());
        }
        udfService.registerFunction(metadata, request.replace(), listener, request.masterNodeTimeout());
    }

    @Override
    protected ClusterBlockException checkBlock(CreateUserDefinedFunctionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
