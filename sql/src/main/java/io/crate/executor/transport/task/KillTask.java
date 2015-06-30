/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.JobTask;
import io.crate.executor.RowCountResult;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.KillAllRequest;
import io.crate.executor.transport.kill.KillResponse;
import io.crate.executor.transport.kill.TransportKillAllNodeAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class KillTask extends JobTask {

    private final SettableFuture<TaskResult> result;
    private final List<ListenableFuture<TaskResult>> results;
    private ClusterService clusterService;
    private TransportKillAllNodeAction transportKillAllNodeAction;

    public KillTask(ClusterService clusterService,
                    TransportKillAllNodeAction transportKillAllNodeAction,
                    UUID jobId) {
        super(jobId);
        this.clusterService = clusterService;
        this.transportKillAllNodeAction = transportKillAllNodeAction;
        result = SettableFuture.create();
        results = ImmutableList.of((ListenableFuture<TaskResult>) result);
    }

    @Override
    public void start() {
        DiscoveryNodes nodes = clusterService.state().nodes();
        KillAllRequest request = new KillAllRequest();
        final AtomicInteger counter = new AtomicInteger(nodes.size());
        final AtomicLong numKilled = new AtomicLong(0);
        final AtomicReference<Throwable> lastThrowable = new AtomicReference<>();

        for (DiscoveryNode node : nodes) {
            transportKillAllNodeAction.execute(node.id(), request, new ActionListener<KillResponse>() {
                @Override
                public void onResponse(KillResponse killResponse) {
                    numKilled.addAndGet(killResponse.numKilled());
                    countdown();
                }

                @Override
                public void onFailure(Throwable e) {
                    lastThrowable.set(e);
                    countdown();
                }

                private void countdown() {
                    if (counter.decrementAndGet() == 0) {
                        Throwable throwable = lastThrowable.get();
                        if (throwable == null) {
                            result.set(new RowCountResult(numKilled.get()));
                        } else {
                            result.setException(throwable);
                        }
                    }
                }
            });
        }
    }

    @Override
    public List<ListenableFuture<TaskResult>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<? extends ListenableFuture<TaskResult>> result) {
        // ignore
    }
}
