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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.analyze.where.DocKeys;
import io.crate.executor.JobTask;
import io.crate.executor.TaskResult;
import io.crate.jobs.ESJobContext;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.planner.node.dml.ESDeleteNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.index.engine.VersionConflictEngineException;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;

public class ESDeleteTask extends JobTask {

    private final List<ListenableFuture<TaskResult>> resultList;
    private final JobExecutionContext context;

    public ESDeleteTask(UUID jobId,
                        ESDeleteNode node,
                        TransportDeleteAction transport,
                        JobContextService jobContextService) {
        super(jobId);
        resultList = new ArrayList<>(node.docKeys().size());
        List<DeleteRequest> requests = new ArrayList<>(node.docKeys().size());
        List<ActionListener> listeners = new ArrayList<>(node.docKeys().size());
        for (DocKeys.DocKey docKey : node.docKeys()) {
            DeleteRequest request = new DeleteRequest(
                    ESGetTask.indexName(node.tableInfo(), docKey.partitionValues()),
                    Constants.DEFAULT_MAPPING_TYPE, docKey.id());
            request.routing(docKey.routing());
            if (docKey.version().isPresent()) {
                request.version(docKey.version().get());
            }
            requests.add(request);
            SettableFuture<TaskResult> result = SettableFuture.create();
            resultList.add(result);
            listeners.add(new DeleteResponseListener(result));
        }

        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(jobId());
        ESJobContext esJobContext = new ESJobContext("delete", requests, listeners, resultList, transport, null);
        contextBuilder.addSubContext(node.executionPhaseId(), esJobContext);
        context = jobContextService.createContext(contextBuilder);
    }

    @Override
    public void start() {
        context.start();
    }

    @Override
    public List<? extends ListenableFuture<TaskResult>> result() {
        return resultList;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "upstreamResult not supported on %s",
                        getClass().getSimpleName()));
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
}
