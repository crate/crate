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

package io.crate.executor.transport.task;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.NodeCollectRequest;
import io.crate.executor.transport.NodeCollectResponse;
import io.crate.executor.transport.TransportCollectNodeAction;
import io.crate.operation.collect.HandlerSideDataCollectOperation;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.action.ActionListener;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class RemoteCollectTask extends Task<QueryResult> {

    private final CollectNode collectNode;
    private final List<ListenableFuture<QueryResult>> result;
    private final String[] nodeIds;
    private final TransportCollectNodeAction transportCollectNodeAction;
    private final HandlerSideDataCollectOperation handlerSideDataCollectOperation;

    public RemoteCollectTask(UUID jobId,
                             CollectNode collectNode,
                             TransportCollectNodeAction transportCollectNodeAction,
                             HandlerSideDataCollectOperation handlerSideDataCollectOperation) {
        super(jobId);
        this.collectNode = collectNode;
        this.transportCollectNodeAction = transportCollectNodeAction;
        this.handlerSideDataCollectOperation = handlerSideDataCollectOperation;

        Preconditions.checkArgument(collectNode.isRouted(),
                "RemoteCollectTask currently only works for plans with routing"
        );

        Preconditions.checkArgument(collectNode.routing().hasLocations(), "RemoteCollectTask does not need to be executed.");


        int resultSize = collectNode.routing().nodes().size();
        nodeIds = collectNode.routing().nodes().toArray(new String[resultSize]);
        result = new ArrayList<>(resultSize);
        for (int i = 0; i < resultSize; i++) {
            result.add(SettableFuture.<QueryResult>create());
        }
    }

    @Override
    public void start() {
        NodeCollectRequest request = new NodeCollectRequest(collectNode);
        for (int i = 0; i < nodeIds.length; i++) {
            final int resultIdx = i;

            if (nodeIds[i] == null) {
                handlerSideCollect(resultIdx);
                continue;
            }

            transportCollectNodeAction.execute(
                    nodeIds[i],
                    request,
                    new ActionListener<NodeCollectResponse>() {
                        @Override
                        public void onResponse(NodeCollectResponse response) {
                            ((SettableFuture<QueryResult>)result.get(resultIdx)).set(new QueryResult(response.rows()));
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            ((SettableFuture<QueryResult>)result.get(resultIdx)).setException(e);
                        }
                    }
            );
        }
    }

    private void handlerSideCollect(final int resultIdx) {
        ListenableFuture<Object[][]> future = handlerSideDataCollectOperation.collect(collectNode);
        Futures.addCallback(future, new FutureCallback<Object[][]>() {
            @Override
            public void onSuccess(@Nullable Object[][] rows) {
                ((SettableFuture<QueryResult>)result.get(resultIdx)).set(new QueryResult(rows));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                ((SettableFuture<QueryResult>)result.get(resultIdx)).setException(t);
            }
        });
    }

    @Override
    public List<ListenableFuture<QueryResult>> result() {
        return result;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        throw new UnsupportedOperationException("nope");
    }
}
