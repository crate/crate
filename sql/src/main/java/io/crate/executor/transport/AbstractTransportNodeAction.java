/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.executor.transport;

import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;

public abstract class AbstractTransportNodeAction<TRequest extends TransportRequest, TResponse extends TransportResponse> {

    private final ESLogger logger = Loggers.getLogger(getClass());
    private final TransportService transportService;

    public AbstractTransportNodeAction(TransportService transportService) {
        this.transportService = transportService;

        transportService.registerHandler(transportAction(), new TransportHandler());
    }

    protected String executorName() {
        return ThreadPool.Names.SEARCH;
    }

    public void execute(String targetNode, TRequest request, ActionListener<TResponse> listener) {
        createAsyncAction(targetNode, request, listener).start();
    }

    abstract protected AsyncAction createAsyncAction(
            String targetNode, TRequest request, ActionListener<TResponse> listener);
    abstract protected TRequest createRequest();
    abstract protected String transportAction();
    abstract protected ListenableActionFuture<TResponse> nodeOperation(final TRequest request) throws CrateException;

    private class TransportHandler extends BaseTransportRequestHandler<TRequest> {

        @Override
        public TRequest newInstance() {
            return createRequest();
        }

        @Override
        public void messageReceived(final TRequest request, final TransportChannel channel) throws Exception {
            try {
                nodeOperation(request).addListener(new ActionListener<TResponse>() {
                    @Override
                    public void onResponse(TResponse response) {
                        try {
                            channel.sendResponse(response);
                        } catch (IOException e) {
                            logger.error("Error sending node response", e);
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            channel.sendResponse(e);
                        } catch (IOException e1) {
                            logger.error("Error sending node failure", e1);
                        }
                    }
                });
            } catch (CrateException e) {
                channel.sendResponse(e);
            }
        }

        @Override
        public String executor() {
            return executorName();
        }
    }

    public interface AsyncAction {

        public void start();
    }
}
