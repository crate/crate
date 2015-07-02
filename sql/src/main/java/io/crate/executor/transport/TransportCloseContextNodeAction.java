/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import io.crate.exceptions.Exceptions;
import io.crate.jobs.ExecutionSubContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.operation.collect.StatsTables;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Transport handler for closing a context(lucene) for a complete job.
 * This is currently ONLY used by the {@link io.crate.operation.projectors.FetchProjector}
 * and should NOT be re-used somewhere else.
 * We will refactor this architecture later on, so a fetch projection will only close related
 * operation contexts and NOT a whole job context.
 * (This requires a refactoring of the {@link JobContextService} architecture)
 */
@Singleton
public class TransportCloseContextNodeAction implements NodeAction<NodeCloseContextRequest, NodeCloseContextResponse> {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportCloseContextNodeAction.class);

    private final String transportAction = "crate/sql/node/context/close";
    private final Transports transports;
    private final StatsTables statsTables;
    private final JobContextService jobContextService;

    @Inject
    public TransportCloseContextNodeAction(TransportService transportService,
                                           Transports transports,
                                           StatsTables statsTables,
                                           JobContextService jobContextService) {
        this.transports = transports;
        this.statsTables = statsTables;
        this.jobContextService = jobContextService;
        transportService.registerHandler(transportAction, new NodeActionRequestHandler<NodeCloseContextRequest, NodeCloseContextResponse>(this) {
            @Override
            public NodeCloseContextRequest newInstance() {
                return new NodeCloseContextRequest();
            }
        });
    }

    public void execute(
            String targetNode,
            NodeCloseContextRequest request,
            ActionListener<NodeCloseContextResponse> listener) {
        transports.executeLocalOrWithTransport(this, targetNode, request, listener,
                new DefaultTransportResponseHandler<NodeCloseContextResponse>(listener, executorName()) {
                    @Override
                    public NodeCloseContextResponse newInstance() {
                        return new NodeCloseContextResponse();
                    }
                });
    }

    @Override
    public String actionName() {
        return transportAction;
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.SEARCH;
    }

    @Override
    public void nodeOperation(final NodeCloseContextRequest request,
                              final ActionListener<NodeCloseContextResponse> response) {
        statsTables.operationStarted(request.executionPhaseId(), request.jobId(), "closeContext");
        try {
            JobExecutionContext jobExecutionContext = jobContextService.getContextOrNull(request.jobId());
            if (jobExecutionContext != null) {
                ExecutionSubContext subContext = jobExecutionContext.getSubContextOrNull(request.executionPhaseId());
                if (subContext != null) {
                    LOGGER.trace("Received CloseContextRequest, closing ExecutionSubContext {}/{}",
                            request.jobId(), request.executionPhaseId());
                    subContext.close();
                }
            }
            statsTables.operationFinished(request.executionPhaseId(), null, 0L);
            response.onResponse(new NodeCloseContextResponse());
        } catch (Throwable t) {
            statsTables.operationFinished(request.executionPhaseId(), Exceptions.messageOf(t), 0L);
            response.onFailure(t);
        }
    }
}
