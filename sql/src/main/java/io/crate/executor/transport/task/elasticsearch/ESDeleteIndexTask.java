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

import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.AbstractChainedTask;
import io.crate.planner.node.ddl.ESDeleteIndexNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.List;

public class ESDeleteIndexTask extends AbstractChainedTask<RowCountResult> {

    private static final RowCountResult RESULT = TaskResult.ONE_ROW;
    private static final RowCountResult RESULT_PARTITION = TaskResult.ROW_COUNT_UNKNOWN;

    private final TransportDeleteIndexAction transport;
    private final DeleteIndexRequest request;
    private final ActionListener<DeleteIndexResponse> listener;

    static class DeleteIndexListener implements ActionListener<DeleteIndexResponse> {

        private final SettableFuture<RowCountResult> future;
        private final boolean isPartition;

        DeleteIndexListener(SettableFuture<RowCountResult> future, boolean isPartition) {
            this.future = future;
            this.isPartition = isPartition;
        }

        @Override
        public void onResponse(DeleteIndexResponse deleteIndexResponse) {
            if (isPartition) {
                future.set(RESULT_PARTITION);
            } else {
                future.set(RESULT);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    public ESDeleteIndexTask(TransportDeleteIndexAction transport, ESDeleteIndexNode node) {
        super();
        this.transport = transport;
        this.request = new DeleteIndexRequest(node.index());
        this.request.indicesOptions(IndicesOptions.strictExpandOpen());
        this.listener = new DeleteIndexListener(result, node.isPartition());
    }

    @Override
    protected void doStart(List<TaskResult> upstreamResults) {
        transport.execute(request, listener);
    }
}
