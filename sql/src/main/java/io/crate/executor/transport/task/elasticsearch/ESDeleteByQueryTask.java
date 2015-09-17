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
import io.crate.analyze.WhereClause;
import io.crate.executor.TaskResult;
import io.crate.jobs.JobContextService;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ESDeleteByQueryTask extends EsJobContextTask {

    private final static ESQueryBuilder QUERY_BUILDER = new ESQueryBuilder();

    public ESDeleteByQueryTask(UUID jobId,
                               ESDeleteByQueryNode node,
                               TransportDeleteByQueryAction transport,
                               JobContextService jobContextService) {
        super(jobId, node.executionPhaseId(), node.whereClauses().size(), jobContextService);
        List<DeleteByQueryRequest> requests = new ArrayList<>(node.whereClauses().size());
        List<ActionListener> listeners = new ArrayList<>(node.whereClauses().size());

        for (int i = 0; i < node.whereClauses().size(); i++) {
            DeleteByQueryRequest request = new DeleteByQueryRequest();
            SettableFuture<TaskResult> result = SettableFuture.create();
            String[] indices = node.indices().get(i);
            WhereClause whereClause = node.whereClauses().get(i);
            String routing = node.routings().get(i);
            try {
                request.source(QUERY_BUILDER.convert(whereClause));
                request.indices(indices);
                if (whereClause.clusteredBy().isPresent()){
                    request.routing(routing);
                }
            } catch (IOException e) {
                result.setException(e);
            }
            results.add(result);
            requests.add(request);
            listeners.add(new Listener(result));
        }

        createContext("delete by query", requests, listeners, transport, null);
    }

    static class Listener implements ActionListener<DeleteByQueryResponse> {

        protected final SettableFuture<TaskResult> result;

        public Listener(SettableFuture<TaskResult> result) {
            this.result = result;
        }

        @Override
        public void onResponse(DeleteByQueryResponse indexDeleteByQueryResponses) {
            result.set(TaskResult.ROW_COUNT_UNKNOWN);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }
}
