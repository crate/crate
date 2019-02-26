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

package io.crate.cluster.decommission;

import io.crate.cluster.gracefulstop.DecommissioningService;
import io.crate.concurrent.CompletableFutures;
import io.crate.execution.support.NodeAction;
import io.crate.execution.support.NodeActionRequestHandler;
import io.crate.execution.support.Transports;
import io.crate.es.action.ActionListener;
import io.crate.es.action.ActionListenerResponseHandler;
import io.crate.es.action.support.master.AcknowledgedResponse;
import io.crate.es.common.inject.Inject;
import io.crate.es.common.inject.Singleton;
import io.crate.es.threadpool.ThreadPool;
import io.crate.es.transport.TransportService;

import java.util.concurrent.CompletableFuture;

@Singleton
public class TransportDecommissionNodeAction implements NodeAction<DecommissionNodeRequest, AcknowledgedResponse> {

    private static final String ACTION_NAME = "internal:crate:sql/decommission/node";
    private static final String EXECUTOR = ThreadPool.Names.MANAGEMENT;

    private final DecommissioningService decommissioningService;
    private final Transports transports;

    @Inject
    public TransportDecommissionNodeAction(TransportService transportService,
                                           DecommissioningService decommissioningService,
                                           Transports transports) {
        this.decommissioningService = decommissioningService;
        this.transports = transports;

        transportService.registerRequestHandler(ACTION_NAME,
            () -> DecommissionNodeRequest.INSTANCE,
            EXECUTOR,
            new NodeActionRequestHandler<>(this)
        );
    }

    public void execute(final String nodeId,
                        final DecommissionNodeRequest request,
                        final ActionListener<AcknowledgedResponse> listener) {
        transports.sendRequest(
            ACTION_NAME,
            nodeId,
            request,
            listener,
            new ActionListenerResponseHandler<>(listener, AcknowledgedResponse::new)
        );
    }

    @Override
    public CompletableFuture<AcknowledgedResponse> nodeOperation(DecommissionNodeRequest request) {
        try {
            return decommissioningService.decommission().thenApply(aVoid -> new AcknowledgedResponse(true));
        } catch (Throwable t) {
            return CompletableFutures.failedFuture(t);
        }
    }
}
