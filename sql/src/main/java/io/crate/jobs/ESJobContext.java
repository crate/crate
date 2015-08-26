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

import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.TaskResult;
import io.crate.operation.projectors.FlatProjectorChain;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class ESJobContext implements ExecutionSubContext, ExecutionState {

    private ContextCallback callback = ContextCallback.NO_OP;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final List<? extends ActionListener> listeners;
    private String operationName;
    private final List<? extends ActionRequest> requests;
    private final List<? extends Future<TaskResult>> resultFutures;
    private final TransportAction transportAction;
    private volatile boolean iskilled = false;
    private final SettableFuture<Void> closeFuture = SettableFuture.create();
    @Nullable
    private final FlatProjectorChain projectorChain;

    private final static ESLogger LOGGER = Loggers.getLogger(ESJobContext.class);


    public ESJobContext(String operationName,
                        List<? extends ActionRequest> requests,
                        List<? extends ActionListener> listeners,
                        List<? extends Future<TaskResult>> resultFutures,
                        TransportAction transportAction,
                        @Nullable FlatProjectorChain projectorChain) {
        this.operationName = operationName;
        this.requests = requests;
        this.listeners = listeners;
        this.resultFutures = resultFutures;
        this.transportAction = transportAction;
        this.projectorChain = projectorChain;
    }

    public void start() {
        if (!closed.get()) {
            if (projectorChain != null) {
                projectorChain.startProjections(this);
            }
            for (int i = 0; i < requests.size(); i++) {
                transportAction.execute(requests.get(i), new InternalActionListener(listeners.get(i), this));
            }
        }
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callback = MultiContextCallback.merge(callback, contextCallback);
    }

    @Override
    public void close() {
        doClose(null);
    }

    void doClose(@Nullable Throwable t) {
        if (!closed.getAndSet(true)) {
            callback.onClose(t, -1L);
            closeFuture.set(null);
        } else {
            try {
                closeFuture.get();
            } catch (Throwable e) {
                LOGGER.warn("Error while waiting for already running close {}", e);
            }
        }
    }

    @Override
    public void kill() {
        iskilled = true;
        for (Future<?> resultFuture : resultFutures) {
            resultFuture.cancel(true);
        }
        doClose(new CancellationException());
    }

    public String name() {
        return operationName;
    }

    @Override
    public boolean isKilled() {
        return iskilled;
    }

    private static class InternalActionListener implements ActionListener {

        private final ActionListener listener;
        private final ESJobContext context;

        public InternalActionListener (ActionListener listener, ESJobContext context) {
            this.listener = listener;
            this.context = context;
        }

        @Override
        public void onResponse(Object o) {
            context.doClose(null);
            listener.onResponse(o);
        }

        @Override
        public void onFailure(Throwable e) {
            context.doClose(e);
            listener.onFailure(e);
        }
    }
}
