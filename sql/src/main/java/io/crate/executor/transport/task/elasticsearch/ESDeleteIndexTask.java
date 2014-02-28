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

package io.crate.executor.transport.task.elasticsearch;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.Arrays;
import java.util.List;

public class ESDeleteIndexTask implements Task<Object[][]> {

    private static final Object[][] RESULT = new Object[][]{ new Object[]{1l}};

    private final TransportDeleteIndexAction transport;
    private final DeleteIndexRequest request;
    private final List<ListenableFuture<Object[][]>> results;
    private final ActionListener<DeleteIndexResponse> listener;

    static class DeleteIndexListener implements ActionListener<DeleteIndexResponse> {

        private final SettableFuture<Object[][]> future;

        DeleteIndexListener(SettableFuture<Object[][]> future) {
            this.future = future;
        }

        @Override
        public void onResponse(DeleteIndexResponse deleteIndexResponse) {
            future.set(RESULT);
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    public ESDeleteIndexTask(TransportDeleteIndexAction transport, ESDeleteIndexNode node) {
        this.transport = transport;
        this.request = new DeleteIndexRequest(node.index());
        this.request.indicesOptions(IndicesOptions.strict());
        SettableFuture <Object[][]> result = SettableFuture.create();
        this.listener = new DeleteIndexListener(result);
        this.results = Arrays.<ListenableFuture<Object[][]>>asList(result);
    }

    @Override
    public void start() {
        this.transport.execute(this.request, this.listener);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return this.results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException();
    }
}
