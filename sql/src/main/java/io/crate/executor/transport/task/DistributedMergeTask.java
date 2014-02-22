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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.executor.transport.merge.NodeMergeRequest;
import io.crate.executor.transport.merge.NodeMergeResponse;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.planner.node.MergeNode;
import org.cratedb.sql.CrateException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;

import java.util.ArrayList;
import java.util.List;

public class DistributedMergeTask implements Task<Object[][]> {

    private final MergeNode mergeNode;
    private final TransportMergeNodeAction transportMergeNodeAction;
    private final ArrayList<ListenableFuture<Object[][]>> results;
    private List<ListenableFuture<Object[][]>> upstreamResult;
    private final ThreadPool threadPool;

    public DistributedMergeTask(ThreadPool threadPool, TransportMergeNodeAction transportMergeNodeAction, MergeNode mergeNode) {
        this.threadPool = threadPool;
        Preconditions.checkNotNull(mergeNode.executionNodes());

        this.transportMergeNodeAction = transportMergeNodeAction;
        this.mergeNode = mergeNode;

        results = new ArrayList<>(mergeNode.executionNodes().size());
        for (String node : mergeNode.executionNodes()) {
            results.add(SettableFuture.<Object[][]>create());
        }
    }

    @Override
    public void start() {
        int i = 0;
        final NodeMergeRequest request = new NodeMergeRequest(mergeNode);

        // if we have an upstream result, we need to register failure handlers here, in order to
        // handle bootstrapping failures on the data providing side
        if (upstreamResult != null){
            // consume the upstream result
            for (final ListenableFuture<Object[][]> upstreamFuture : upstreamResult) {
                upstreamFuture.addListener(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            upstreamFuture.get();
                        } catch (Exception e) {
                            for (ListenableFuture<Object[][]> result : results) {
                                //result.cancel(true);
                            ((SettableFuture<Object[][]>)result).setException(e.getCause());
                            }
                            throw new CrateException("Failure in collect result", e);
                        }
                    }
                }, threadPool.executor(ThreadPool.Names.GENERIC));
            }
        }

        for (String node : mergeNode.executionNodes()) {
            final int resultIdx = i;
            transportMergeNodeAction.startMerge(node, request, new ActionListener<NodeMergeResponse>() {

                @Override
                public void onResponse(NodeMergeResponse nodeMergeResponse) {
                    ((SettableFuture<Object[][]>)results.get(resultIdx)).set(nodeMergeResponse.rows());
                }

                @Override
                public void onFailure(Throwable e) {
                    if (e instanceof RemoteTransportException) {
                        e = e.getCause();
                    }
                    SettableFuture<Object[][]> result = (SettableFuture<Object[][]>) results.get(resultIdx);
                    result.setException(e);
                }
            });

            i++;
        }
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        upstreamResult = result;
    }
}
