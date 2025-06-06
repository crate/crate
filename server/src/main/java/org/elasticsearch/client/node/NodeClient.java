/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.node;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportResponse;

/**
 * Client that executes actions on the local node.
 */
public class NodeClient implements Client {

    @SuppressWarnings("rawtypes")
    private Map<ActionType, TransportAction> actions;

    public NodeClient() {
    }

    @SuppressWarnings("rawtypes")
    public void initialize(Map<ActionType, TransportAction> actions) {
        this.actions = actions;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    @SuppressWarnings("unchecked")
    public <Req extends TransportRequest, Resp extends TransportResponse> CompletableFuture<Resp> execute(ActionType<Resp> action, Req request) {
        if (actions == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        TransportAction<Req, Resp> transportAction = actions.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction.execute(request);
    }
}
