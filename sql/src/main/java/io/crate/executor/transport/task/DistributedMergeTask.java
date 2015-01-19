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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.exceptions.UnknownUpstreamFailure;
import io.crate.executor.JobTask;
import io.crate.executor.QueryResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.merge.NodeMergeRequest;
import io.crate.executor.transport.merge.NodeMergeResponse;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.planner.node.dql.MergeNode;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.transport.RemoteTransportException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DistributedMergeTask extends JobTask {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final MergeNode mergeNode;
    private final TransportMergeNodeAction transportMergeNodeAction;
    private final ArrayList<ListenableFuture<TaskResult>> results;
    private List<ListenableFuture<TaskResult>> upstreamResult;

    public DistributedMergeTask(UUID jobId,
                                TransportMergeNodeAction transportMergeNodeAction,
                                MergeNode mergeNode) {
        super(jobId);
        Preconditions.checkNotNull(mergeNode.executionNodes());

        this.transportMergeNodeAction = transportMergeNodeAction;
        this.mergeNode = mergeNode;

        results = new ArrayList<>(mergeNode.executionNodes().size());
        for (String node : mergeNode.executionNodes()) {
            results.add(SettableFuture.<TaskResult>create());
        }
    }

    @Override
    public void start() {
        logger.trace("start");
        int i = 0;
        final NodeMergeRequest request = new NodeMergeRequest(mergeNode);
        final CountDownLatch countDownLatch;

        // if we have an upstream result, we need to register failure handlers here, in order to
        // handle bootstrapping failures on the data providing side
        if (upstreamResult == null){
            countDownLatch = new CountDownLatch(0);
        } else  {
            countDownLatch = new CountDownLatch(upstreamResult.size());
            logger.trace("upstreamResult.size: " + upstreamResult.size());
            // consume the upstream result
            for (final ListenableFuture<TaskResult> upstreamFuture : upstreamResult) {
                Futures.addCallback(upstreamFuture, new FutureCallback<TaskResult>() {
                    @Override
                    public void onSuccess(@Nullable TaskResult result) {
                        assert result != null;
                        logger.trace("successful collect Future size: " + result.rows().length);
                        countDownLatch.countDown();
                    }

                    @Override
                    public void onFailure(@Nonnull Throwable t) {
                        if (t instanceof RemoteTransportException){
                            t = t.getCause();
                        }
                        for (ListenableFuture<TaskResult> result : results) {
                            ((SettableFuture<TaskResult>)result).setException(t);
                        }
                        countDownLatch.countDown();
                        logger.error("remote collect request failed", t);
                    }
                });
            }
        }

        for (final String node : mergeNode.executionNodes()) {
            logger.trace("prepare executionNode: {} of {}", node, mergeNode.executionNodes().size());
            final int resultIdx = i;
            transportMergeNodeAction.startMerge(node, request, new ActionListener<NodeMergeResponse>() {

                @Override
                public void onResponse(NodeMergeResponse nodeMergeResponse) {
                    logger.trace("startMerge.onResponse: {} of {}", node, mergeNode.executionNodes().size());
                    ((SettableFuture<TaskResult>)results.get(resultIdx)).set(new QueryResult(nodeMergeResponse.rows()));
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.trace("startMerge.onFailure: {} of {}", node, mergeNode.executionNodes().size());
                    if (e instanceof RemoteTransportException) {
                        e = e.getCause();
                    }
                    if (e instanceof UnknownUpstreamFailure) {
                        // prioritize the upstreams original exception over the UnknownUpstreamFailure
                        try {
                            countDownLatch.await(1, TimeUnit.SECONDS);
                        } catch (InterruptedException e1) {
                            logger.error("Interrupted waiting for upstream exception", e1);
                        }
                    }
                    SettableFuture<TaskResult> result = (SettableFuture<TaskResult>) results.get(resultIdx);
                    result.setException(e);
                    logger.error("Failure getting NodeMergeResponse: ", e);
                }
            });

            i++;
        }
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        logger.trace("result() size:{}", results.size());
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<TaskResult>> result) {
        upstreamResult = result;
    }
}
