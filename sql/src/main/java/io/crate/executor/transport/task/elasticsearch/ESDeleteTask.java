/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.planner.node.ESDeleteNode;
import org.cratedb.Constants;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;

import java.util.Arrays;
import java.util.List;

public class ESDeleteTask implements Task<Object[][]> {

    private final List<ListenableFuture<Object[][]>> results;
    private final TransportDeleteAction transport;
    private final DeleteRequest request;
    private final ActionListener<DeleteResponse> listener;

    public ESDeleteTask(TransportDeleteAction transport, ESDeleteNode node) {
        Preconditions.checkNotNull(transport);
        Preconditions.checkNotNull(node);

        this.transport = transport;

        final SettableFuture<Object[][]> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<Object[][]>>asList(result);


        request = new DeleteRequest(node.index(), Constants.DEFAULT_MAPPING_TYPE, node.id());
        listener = new DeleteResponseListener(result);
    }

    static class DeleteResponseListener implements ActionListener<DeleteResponse> {

        private final SettableFuture<Object[][]> result;

        public DeleteResponseListener(SettableFuture<Object[][]> result) {
            this.result = result;
        }

        @Override
        public void onResponse(DeleteResponse response) {
            // TODO: check for response.isNotFound() and set affected rows according to this
            result.set(Constants.EMPTY_RESULT);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    @Override
    public void start() {
        transport.execute(request, listener);
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        throw new UnsupportedOperationException();
    }

}
