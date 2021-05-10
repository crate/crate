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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

@Singleton
public class TransportDropUserDefinedFunctionAction
    extends TransportMasterNodeAction<DropUserDefinedFunctionRequest, AcknowledgedResponse> {

    private final UserDefinedFunctionService udfService;

    @Inject
    public TransportDropUserDefinedFunctionAction(TransportService transportService,
                                                  ClusterService clusterService,
                                                  ThreadPool threadPool,
                                                  UserDefinedFunctionService udfService,
                                                  IndexNameExpressionResolver indexNameExpressionResolver) {
        super("internal:crate:sql/udf/drop",
            transportService,
            clusterService,
            threadPool,
            DropUserDefinedFunctionRequest::new,
            indexNameExpressionResolver
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
    protected void masterOperation(Task task,
                                   final DropUserDefinedFunctionRequest request,
                                   ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) throws Exception {
        udfService.dropFunction(
            request.schema(),
            request.name(),
            request.argumentTypes(),
            request.ifExists(),
            listener,
            request.masterNodeTimeout()
        );
    }

    @Override
    protected ClusterBlockException checkBlock(DropUserDefinedFunctionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
