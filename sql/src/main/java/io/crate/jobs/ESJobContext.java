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

package io.crate.jobs;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ESJobContext extends AbstractExecutionSubContext {

    private static final Logger LOGGER = Loggers.getLogger(ESJobContext.class);

    private final List<? extends ActionListener> listeners;
    private String operationName;
    private final List<? extends ActionRequest> requests;
    private final List<CompletableFuture<Long>> resultFutures;
    private final TransportAction transportAction;

    public ESJobContext(int id,
                        String operationName,
                        List<? extends ActionRequest> requests,
                        List<? extends ActionListener> listeners,
                        List<CompletableFuture<Long>> resultFutures,
                        TransportAction transportAction) {
        super(id, LOGGER);
        this.operationName = operationName;
        this.requests = requests;
        this.listeners = listeners;
        this.resultFutures = resultFutures;
        this.transportAction = transportAction;
    }

    @Override
    protected void innerStart() {
        for (int i = 0; i < requests.size(); i++) {
            transportAction.execute(requests.get(i), new InternalActionListener(listeners.get(i), this));
        }
    }

    @Override
    protected void innerKill(@Nonnull Throwable t) {
        for (CompletableFuture<Long> resultFuture : resultFutures) {
            resultFuture.cancel(true);
        }
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        if (t != null) {
            for (CompletableFuture<Long> resultFuture : resultFutures) {
                if (!resultFuture.isDone()) {
                    resultFuture.completeExceptionally(t);
                }
            }
        }
    }

    public String name() {
        return operationName;
    }

    private static class InternalActionListener implements ActionListener {

        private final ActionListener listener;
        private final ESJobContext context;

        InternalActionListener(ActionListener listener, ESJobContext context) {
            this.listener = listener;
            this.context = context;
        }

        @Override
        public void onResponse(Object o) {
            //noinspection unchecked
            listener.onResponse(o);
            context.close(null);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
            context.close(e);
        }
    }
}
