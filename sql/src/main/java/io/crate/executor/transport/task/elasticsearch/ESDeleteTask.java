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

import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.AsyncChainedTask;
import io.crate.planner.node.dml.ESDeleteNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.UUID;

public class ESDeleteTask extends AsyncChainedTask {

    private final TransportDeleteAction transport;
    private final DeleteRequest request;
    private final ActionListener<DeleteResponse> listener;

    public ESDeleteTask(UUID jobId, ESDeleteNode node, TransportDeleteAction transport) {
        super(jobId);
        this.transport = transport;
        request = new DeleteRequest(
                ESGetTask.indexName(node.tableInfo(), node.key().partitionValues()),
                Constants.DEFAULT_MAPPING_TYPE, node.key().id());
        request.routing(node.key().routing());
        if (node.key().version().isPresent()) {
            request.version(node.key().version().get());
        }
        listener = new DeleteResponseListener(result);
    }

    static class DeleteResponseListener implements ActionListener<DeleteResponse> {

        private final SettableFuture<TaskResult> result;

        public DeleteResponseListener(SettableFuture<TaskResult> result) {
            this.result = result;
        }

        @Override
        public void onResponse(DeleteResponse response) {
            if (!response.isFound()) {
                result.set(TaskResult.ZERO);
            } else {
                result.set(TaskResult.ONE_ROW);
            }
        }

        @Override
        public void onFailure(Throwable e) {
            Throwable cause = e.getCause();
            // if the delete Operation was done locally (on the same node) e is the real exception
            // otherwise the exception is wrapped inside a transportExecutionException
            if (e instanceof VersionConflictEngineException || (cause != null && cause instanceof VersionConflictEngineException)) {
                // treat version conflict as rows affected = 0
                result.set(TaskResult.ZERO);
            } else {
                result.setException(e);
            }
        }
    }

    @Override
    public void start() {
        transport.execute(request, listener);
    }
}
