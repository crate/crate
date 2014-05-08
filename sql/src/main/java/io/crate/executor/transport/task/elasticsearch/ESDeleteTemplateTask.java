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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.exceptions.TaskExecutionException;
import io.crate.executor.Task;
import io.crate.planner.node.ddl.ESDeleteTemplateNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateResponse;
import org.elasticsearch.action.admin.indices.template.delete.TransportDeleteIndexTemplateAction;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ESDeleteTemplateTask implements Task<Object[][]> {

    private static class DeleteTemplateListener implements ActionListener<DeleteIndexTemplateResponse> {

        private final SettableFuture<Object[][]> future;

        private DeleteTemplateListener(SettableFuture<Object[][]> future) {
            this.future = future;
        }

        @Override
        public void onResponse(DeleteIndexTemplateResponse response) {
            long count = response.isAcknowledged() ? 1L : 0L;
            future.set(new Object[][]{ new Object[] { count } });
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    private final List<ListenableFuture<Object[][]>> results;
    private final TransportDeleteIndexTemplateAction transport;
    private final DeleteIndexTemplateRequest request;
    private final DeleteTemplateListener listener;

    private List<ListenableFuture<Object[][]>> upstreamResult = ImmutableList.of();

    public ESDeleteTemplateTask(ESDeleteTemplateNode node, TransportDeleteIndexTemplateAction transport) {
        this.transport = transport;
        SettableFuture<Object[][]> result = SettableFuture.create();
        this.results = Arrays.<ListenableFuture<Object[][]>>asList(result);
        this.listener = new DeleteTemplateListener(result);
        this.request = new DeleteIndexTemplateRequest(node.templateName());
    }

    @Override
    public void start() {
        if (!upstreamResult.isEmpty()) {
            try {
                Futures.allAsList(upstreamResult).get();
            } catch (ExecutionException | InterruptedException e) {
                throw new TaskExecutionException(this, e);
            }
        }
        transport.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        this.upstreamResult = result;
    }
}
