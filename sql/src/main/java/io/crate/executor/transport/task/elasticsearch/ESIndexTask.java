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
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;

import java.util.List;

public class ESIndexTask extends AbstractESIndexTask<RowCountResult> {

    private final TransportIndexAction transport;
    private final IndexRequest request;

    private final ActionListener<IndexResponse> listener;

    static class IndexResponseListener implements ActionListener<IndexResponse> {
        private final SettableFuture<RowCountResult> result;

        IndexResponseListener(SettableFuture<RowCountResult> result) {
            this.result = result;
        }

        @Override
        public void onResponse(IndexResponse indexResponse) {
            result.set(TaskResult.ONE_ROW);
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }

    /**
     * @param transport         the transportAction to run the actual ES operation on
     * @param node              the plan node
     */
    public ESIndexTask(TransportIndexAction transport,
                       ESIndexNode node) {
        super(node);
        this.transport = transport;

        assert node.indices().length == 1 : "invalid number of indices";
        List<String> routingValues = this.node.routingValues();
        request = buildIndexRequest(this.node.indices()[0],
                this.node.sourceMaps().get(0),
                this.node.ids().get(0),
                routingValues == null ? null : routingValues.get(0)
        );
        listener = new IndexResponseListener(result);

    }

    @Override
    protected void doStart(List<TaskResult> upstreamResults) {
        transport.execute(request, listener);
    }
}
