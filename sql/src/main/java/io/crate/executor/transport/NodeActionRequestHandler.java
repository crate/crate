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

import io.crate.exceptions.SQLExceptions;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

public final class NodeActionRequestHandler<TRequest extends TransportRequest, TResponse extends TransportResponse>
    implements TransportRequestHandler<TRequest> {

    private final NodeAction<TRequest, TResponse> nodeAction;
    private static final Logger LOGGER = Loggers.getLogger(NodeActionRequestHandler.class);

    public NodeActionRequestHandler(NodeAction<TRequest, TResponse> nodeAction) {
        this.nodeAction = nodeAction;
    }

    @Override
    public void messageReceived(TRequest request, TransportChannel channel) throws Exception {
        nodeAction.nodeOperation(request).whenComplete((result, throwable) -> {
            if (throwable == null) {
                try {
                    channel.sendResponse(result);
                } catch (IOException e) {
                    LOGGER.error("Error sending response: " + e.getMessage(), e);
                }
            } else {
                try {
                    channel.sendResponse((Exception) SQLExceptions.unwrap(throwable));
                } catch (IOException e) {
                    LOGGER.error("Error sending failure: " + e.getMessage(), e);
                }
            }
        });
    }
}
