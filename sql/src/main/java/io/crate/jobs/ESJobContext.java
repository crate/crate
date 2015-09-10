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

import javax.annotation.Nullable;
import java.util.List;
import java.util.concurrent.Future;

public class ESJobContext extends AbstractExecutionSubContext {

    private final List<? extends ActionListener> listeners;
    private String operationName;
    private final List<? extends ActionRequest> requests;
    private final List<SettableFuture<TaskResult>> resultFutures;
    private final TransportAction transportAction;


    @Nullable
    private final FlatProjectorChain projectorChain;

    public ESJobContext(int id,
                        String operationName,
                        List<? extends ActionRequest> requests,
                        List<? extends ActionListener> listeners,
                        List<SettableFuture<TaskResult>> resultFutures,
                        TransportAction transportAction,
                        @Nullable FlatProjectorChain projectorChain) {
        super(id);
        this.operationName = operationName;
        this.requests = requests;
        this.listeners = listeners;
        this.resultFutures = resultFutures;
        this.transportAction = transportAction;
        this.projectorChain = projectorChain;
    }

    @Override
    protected void innerStart() {
        if (projectorChain != null) {
            projectorChain.startProjections(this);
        }
        for (int i = 0; i < requests.size(); i++) {
            transportAction.execute(requests.get(i), new InternalActionListener(listeners.get(i), this));
        }
    }

    @Override
    protected void innerKill(@Nullable Throwable t) {
        for (Future<?> resultFuture : resultFutures) {
            resultFuture.cancel(true);
        }
    }

    @Override
    protected void innerClose(@Nullable Throwable t) {
        if (t != null) {
            for (SettableFuture<TaskResult> resultFuture : resultFutures) {
                if (!resultFuture.isDone()) {
                    resultFuture.setException(t);
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

        public InternalActionListener(ActionListener listener, ESJobContext context) {
            this.listener = listener;
            this.context = context;
        }

        @Override
        public void onResponse(Object o) {
            listener.onResponse(o);
            context.close(null);
        }

        @Override
        public void onFailure(Throwable e) {
            listener.onFailure(e);
            context.close(e);
        }
    }
}
