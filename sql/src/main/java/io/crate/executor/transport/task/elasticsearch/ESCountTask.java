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
import io.crate.exceptions.FailedShardsException;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.planner.node.dql.ESCountNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.count.CountRequest;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.count.TransportCountAction;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ESCountTask implements Task<QueryResult> {

    private final TransportCountAction transportCountAction;
    private final List<ListenableFuture<QueryResult>> results;
    private CountRequest request;
    private ActionListener<CountResponse> listener;
    private final static ESQueryBuilder queryBuilder = new ESQueryBuilder();

    public ESCountTask(ESCountNode node, TransportCountAction transportCountAction) {
        this.transportCountAction = transportCountAction;
        assert node != null;

        final SettableFuture<QueryResult> result = SettableFuture.create();
        results = Arrays.<ListenableFuture<QueryResult>>asList(result);
        // empty partitioned table does not exists
        // or where clause on partitioned table is NO_MATCH
        // shortcut here
        if (node.tableInfo().isPartitioned() &&
                (node.tableInfo().partitions().size() == 0 || node.whereClause().noMatch())) {
            result.set(new QueryResult(new Object[][]{new Object[]{0L}}));
            return;
        }
        request = new CountRequest(node.tableInfo().ident().name());
        listener = new CountResponseListener(result);
        try {
            request.source(queryBuilder.convert(node.whereClause()), false);
        } catch (IOException e) {
            result.setException(e);
        }
    }

    @Override
    public void start() {
        if (!results.get(0).isDone()) {
            transportCountAction.execute(request, listener);
        }
    }

    @Override
    public List<ListenableFuture<QueryResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("ESCountTask does not support upstream results");
    }

    static class CountResponseListener implements ActionListener<CountResponse> {

        private final SettableFuture<QueryResult> result;

        public CountResponseListener(SettableFuture<QueryResult> result) {
            this.result = result;
        }

        @Override
        public void onResponse(CountResponse countResponse) {
            if (countResponse.getFailedShards() > 0) {
                onFailure(new FailedShardsException(countResponse.getShardFailures()));
            } else {
                result.set(new QueryResult(new Object[][]{new Object[]{countResponse.getCount()}}));
            }
        }

        @Override
        public void onFailure(Throwable e) {
            result.setException(e);
        }
    }
}
