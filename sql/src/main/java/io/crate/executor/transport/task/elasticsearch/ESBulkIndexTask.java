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
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;

import java.util.Arrays;
import java.util.List;

public class ESBulkIndexTask extends AbstractESIndexTask {

    private final TransportBulkAction bulkAction;
    private final BulkRequest request;
    private final ActionListener<BulkResponse> listener;

    static class BulkIndexResponseListener implements ActionListener<BulkResponse> {

        private final SettableFuture<Object[][]> result;

        BulkIndexResponseListener(SettableFuture<Object[][]> result) {
            this.result = result;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            BulkItemResponse[] responses = bulkItemResponses.getItems();
            long rowsAffected = 0L;
            for (BulkItemResponse response : responses) {
                if (!response.isFailed()) {
                    rowsAffected++;
                }
            }
            result.set(new Object[][]{new Object[]{rowsAffected}});
        }

        @Override
        public void onFailure(Throwable e) {
            this.result.setException(e);
        }
    }

    public ESBulkIndexTask(TransportBulkAction bulkAction,
                           ESIndexNode node) {
        super(node);
        this.bulkAction = bulkAction;

        this.request = new BulkRequest();

        for (int i = 0; i < this.node.sourceMaps().size(); i++) {
            String[] indices;

            if (node.indices().length == 1) {
                // in case we have only one index for all indexRequests
                String[] arr = new String[node.sourceMaps().size()];
                Arrays.fill(arr, node.indices()[0]);
                indices = arr;
            } else {
                indices = node.indices();
            }

            IndexRequest indexRequest = buildIndexRequest(
                    indices[i],
                    node.sourceMaps().get(i),
                    node.ids().get(i),
                    node.routingValues().get(i));
            this.request.add(indexRequest);
        }

        this.listener = new BulkIndexResponseListener(result);
    }

    @Override
    protected void doStart(List<Object[][]> upstreamResults) {
        this.bulkAction.execute(request, listener);
    }
}
