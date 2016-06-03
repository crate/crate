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
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.planner.node.ddl.ESDeletePartitionNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.delete.TransportDeleteIndexAction;
import org.elasticsearch.action.support.IndicesOptions;

import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class ESDeletePartitionTask extends JobTask {

    private static final TaskResult RESULT_PARTITION = TaskResult.ROW_COUNT_UNKNOWN;

    private final TransportDeleteIndexAction transport;
    private final DeleteIndexRequest request;
    private final ActionListener<DeleteIndexResponse> listener;
    private final SettableFuture<TaskResult> result;
    private final List<? extends ListenableFuture<TaskResult>> results;

    private static class DeleteIndexListener implements ActionListener<DeleteIndexResponse> {

        private final SettableFuture<TaskResult> future;

        DeleteIndexListener(SettableFuture<TaskResult> future) {
            this.future = future;
        }

        @Override
        public void onResponse(DeleteIndexResponse deleteIndexResponse) {
            future.set(RESULT_PARTITION);
        }

        @Override
        public void onFailure(Throwable e) {
            future.setException(e);
        }
    }

    public ESDeletePartitionTask(UUID jobId,
                                 TransportDeleteIndexAction transport,
                                 ESDeletePartitionNode node) {
        super(jobId);
        result = SettableFuture.create();
        results = Collections.singletonList(result);
        this.transport = transport;
        this.request = new DeleteIndexRequest(node.indices());
        if (node.indices().length > 1) {
            /**
             * table is partitioned, in case of concurrent "delete from partitions"
             * it could be that some partitions are already deleted,
             * so ignore it if some are missing
             */
            this.request.indicesOptions(IndicesOptions.lenientExpandOpen());
        } else {
            this.request.indicesOptions(IndicesOptions.strictExpandOpen());
        }
        this.listener = new DeleteIndexListener(result);
    }

    @Override
    public void start() {
        transport.execute(request, listener);
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        return results;
    }
}
