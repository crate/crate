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

import io.crate.action.ActionListeners;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;

public abstract class NodeActionRequestHandler<TRequest extends TransportRequest, TResponse extends TransportResponse>
        extends TransportRequestHandler<TRequest> {

    private final NodeAction<TRequest, TResponse> nodeAction;

    public NodeActionRequestHandler(NodeAction<TRequest, TResponse> nodeAction) {
        this.nodeAction = nodeAction;
    }

    @Override
    public void messageReceived(TRequest request, TransportChannel channel) throws Exception {
        ActionListener<TResponse> actionListener = ActionListeners.forwardTo(channel);
        nodeAction.nodeOperation(request, actionListener);
    }
}
