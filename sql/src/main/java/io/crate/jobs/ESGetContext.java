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

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class ESGetContext implements ExecutionSubContext {

    private final ArrayList<ContextCallback> callbacks = new ArrayList<>(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final ActionListener listener;
    private final ActionRequest request;
    private final TransportAction transportAction;

    public ESGetContext(ActionRequest request,
                        ActionListener listener,
                        TransportAction transportAction) {
        this.request = request;
        this.listener = listener;
        this.transportAction = transportAction;
    }

    public void start() {
        if (!closed.get()) {
            transportAction.execute(request, new InternalActionListener(listener, this));
        }
    }

    @Override
    public void addCallback(ContextCallback contextCallback) {
        callbacks.add(contextCallback);
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            for (ContextCallback callback : callbacks) {
                callback.onClose();
            }
        }
    }

    @Override
    public void kill() {
        throw new UnsupportedOperationException("kill is not implemented");
    }

    private static class InternalActionListener implements ActionListener {

        private final ActionListener listener;
        private final ExecutionSubContext context;

        public InternalActionListener (ActionListener listener, ExecutionSubContext context) {
            this.listener = listener;
            this.context = context;
        }

        @Override
        public void onResponse(Object o) {
            listener.onResponse(o);
            context.close();
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(e);
            context.close();
        }
    }
}
