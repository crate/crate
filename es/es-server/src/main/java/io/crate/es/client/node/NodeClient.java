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

package io.crate.es.client.node;

import io.crate.es.action.Action;
import io.crate.es.action.ActionListener;
import io.crate.es.action.ActionRequest;
import io.crate.es.action.ActionRequestBuilder;
import io.crate.es.action.ActionResponse;
import io.crate.es.action.GenericAction;
import io.crate.es.action.support.TransportAction;
import io.crate.es.client.Client;
import io.crate.es.client.support.AbstractClient;
import io.crate.es.common.settings.Settings;
import io.crate.es.tasks.Task;
import io.crate.es.threadpool.ThreadPool;

import java.util.Map;

/**
 * Client that executes actions on the local node.
 */
public class NodeClient extends AbstractClient {

    private Map<GenericAction, TransportAction> actions;

    public NodeClient(Settings settings, ThreadPool threadPool) {
        super(settings, threadPool);
    }

    public void initialize(Map<GenericAction, TransportAction> actions) {
        this.actions = actions;
    }

    @Override
    public void close() {
        // nothing really to do
    }

    @Override
    public <    Request extends ActionRequest,
                Response extends ActionResponse,
                RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>
            > void doExecute(Action<Request, Response, RequestBuilder> action, Request request, ActionListener<Response> listener) {
        // Discard the task because the Client interface doesn't use it.
        executeLocally(action, request, listener);
    }

    /**
     * Execute an {@link Action} locally, returning that {@link Task} used to track it, and linking an {@link ActionListener}. Prefer this
     * method if you don't need access to the task when listening for the response. This is the method used to implement the {@link Client}
     * interface.
     */
    public <    Request extends ActionRequest,
                Response extends ActionResponse
            > Task executeLocally(GenericAction<Request, Response> action, Request request, ActionListener<Response> listener) {
        return transportAction(action).execute(request, listener);
    }

    /**
     * Get the {@link TransportAction} for an {@link Action}, throwing exceptions if the action isn't available.
     */
    @SuppressWarnings("unchecked")
    private <    Request extends ActionRequest,
                Response extends ActionResponse
            > TransportAction<Request, Response> transportAction(GenericAction<Request, Response> action) {
        if (actions == null) {
            throw new IllegalStateException("NodeClient has not been initialized");
        }
        TransportAction<Request, Response> transportAction = actions.get(action);
        if (transportAction == null) {
            throw new IllegalStateException("failed to find action [" + action + "] to execute");
        }
        return transportAction;
    }
}
