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

package io.crate.execution.engine.profile;

import io.crate.action.FutureActionListener;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.jobs.JobExecutionContext;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Singleton
public class TransportCollectProfileNodeAction implements NodeAction<NodeCollectProfileRequest, NodeCollectProfileResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/node/collectprofile";
    private static final String EXECUTOR = ThreadPool.Names.SEARCH;

    private final Transports transports;
    private final JobContextService jobContextService;

    @Inject
    public TransportCollectProfileNodeAction(TransportService transportService,
                                    Transports transports,
                                    JobContextService jobContextService) {
        this.transports = transports;
        this.jobContextService = jobContextService;

        transportService.registerRequestHandler(
            TRANSPORT_ACTION,
            NodeCollectProfileRequest::new,
            EXECUTOR,
            // force execution because this handler might receive empty close requests which
            // need to be processed to not leak the FetchContext.
            // This shouldn't cause too much of an issue because fetch requests always happen after a query phase.
            // If the threadPool is overloaded the query phase would fail first.
            true,
            false,
            new NodeActionRequestHandler<>(this)
        );
    }

    @Override
    public CompletableFuture<NodeCollectProfileResponse> nodeOperation(NodeCollectProfileRequest request) {
        return CompletableFuture.completedFuture(
            new NodeCollectProfileResponse(collectExecutionTimesAndFinishContext(request.jobId())));
    }

    public void execute(String nodeId,
                        NodeCollectProfileRequest request,
                        FutureActionListener<NodeCollectProfileResponse, Map<String, Long>> listener) {
        transports.sendRequest(TRANSPORT_ACTION, nodeId, request, listener,
            new ActionListenerResponseHandler<>(listener, NodeCollectProfileResponse::new));
    }

    public Map<String, Long> collectExecutionTimesAndFinishContext(UUID jobId) {
        JobExecutionContext context = jobContextService.getContextOrNull(jobId);
        if (context == null) {
            return Collections.emptyMap();
        } else {
            context.finishProfiling();
            return context.executionTimes();
        }
    }
}
