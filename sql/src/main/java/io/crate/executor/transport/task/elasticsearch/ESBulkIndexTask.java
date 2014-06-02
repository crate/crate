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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.planner.node.dml.ESIndexNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class ESBulkIndexTask extends AbstractESIndexTask {

    private final static AtomicInteger currentDelay = new AtomicInteger(0);
    private final TransportBulkAction bulkAction;
    private final BulkRequest request;
    private final ActionListener<BulkResponse> listener;

    static class BulkIndexResponseListener implements ActionListener<BulkResponse> {

        private final SettableFuture<Object[][]> result;
        private final long initialRowsAffected;
        private final ThreadPool threadPool;
        private final TransportBulkAction bulkAction;
        private final BulkRequest request;
        private final ESLogger logger = Loggers.getLogger(getClass());

        BulkIndexResponseListener(ThreadPool threadPool,
                                  TransportBulkAction bulkAction,
                                  BulkRequest request,
                                  SettableFuture<Object[][]> result,
                                  long initialRowsAffected) {
            this.threadPool = threadPool;
            this.bulkAction = bulkAction;
            this.request = request;
            this.result = result;
            this.initialRowsAffected = initialRowsAffected;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            BulkItemResponse[] responses = bulkItemResponses.getItems();
            long rowsAffected = initialRowsAffected;
            IntArrayList toRetry = new IntArrayList();
            String lastFailure = null;

            for (BulkItemResponse response : responses) {
                if (!response.isFailed()) {
                    rowsAffected++;
                } else if (response.getFailureMessage().startsWith("EsRejectedExecution")) {
                    toRetry.add(response.getItemId());
                } else {
                    lastFailure = response.getFailureMessage();
                }
            }

            if (!toRetry.isEmpty()) {
                logger.debug("%s requests failed due to full queue.. retrying", toRetry.size());
                final BulkRequest bulkRequest = new BulkRequest();
                for (IntCursor intCursor : toRetry) {
                    bulkRequest.add(request.requests().get(intCursor.value));
                }

                final long currentRowsAffected = rowsAffected;
                threadPool.schedule(TimeValue.timeValueSeconds(currentDelay.incrementAndGet()),
                        ThreadPool.Names.SAME, new Runnable() {
                            @Override
                            public void run() {
                                bulkAction.execute(bulkRequest, new BulkIndexResponseListener(
                                        threadPool,
                                        bulkAction,
                                        bulkRequest,
                                        result,
                                        currentRowsAffected));
                            }
                        });
            } else if (lastFailure == null) {
                currentDelay.set(0);
                result.set(new Object[][]{new Object[]{rowsAffected}});
            } else {
                currentDelay.set(0);
                result.setException(new RuntimeException(lastFailure));
            }
        }

        @Override
        public void onFailure(Throwable e) {
            this.result.setException(e);
        }
    }

    public ESBulkIndexTask(ThreadPool threadPool,
                           TransportBulkAction bulkAction,
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
        this.listener = new BulkIndexResponseListener(threadPool, bulkAction, request, result, 0L);
    }

    @Override
    protected void doStart(List<Object[][]> upstreamResults) {
        this.bulkAction.execute(request, listener);
    }
}
