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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.exceptions.TaskExecutionException;
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;

import java.util.concurrent.ExecutionException;

public class ESIndexTask extends AbstractESIndexTask {

    private final TransportIndexAction transport;
    private final IndexRequest request;

    private final ActionListener<IndexResponse> listener;

    static class IndexResponseListener implements ActionListener<IndexResponse> {
        public static Object[][] affectedRowsResult = new Object[][]{ new Object[]{ 1L } };
        private final SettableFuture<Object[][]> result;

        IndexResponseListener(SettableFuture<Object[][]> result) {
            this.result = result;
        }

        @Override
        public void onResponse(IndexResponse indexResponse) {
            result.set(affectedRowsResult);
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

        request = buildIndexRequest(this.node.index(),
                this.node.sourceMaps().get(0),
                this.node.ids().get(0),
                this.node.routingValues().get(0)
        );
        listener = new IndexResponseListener(result);

    }

    @Override
    public void start() {
        if (!upStreamResult.isEmpty()) {
            // wait for all upstream results before starting to index
            try {
                Futures.allAsList(upStreamResult).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new TaskExecutionException(this, e);
            }
        }
        transport.execute(request, listener);
    }
}
