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

package io.crate.executor.transport.kill;

import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Singleton
public class TransportKillJobsNodeAction implements NodeAction<KillJobsRequest, KillResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/kill_jobs";

    private JobContextService jobContextService;
    private ClusterService clusterService;
    private Transports transports;

    @Inject
    public TransportKillJobsNodeAction(JobContextService jobContextService,
                                       ClusterService clusterService,
                                       Transports transports,
                                       TransportService transportService) {
        this.jobContextService = jobContextService;
        this.clusterService = clusterService;
        this.transports = transports;
        transportService.registerHandler(TRANSPORT_ACTION, new NodeActionRequestHandler<KillJobsRequest, KillResponse>(this) {
            @Override
            public KillJobsRequest newInstance() {
                return new KillJobsRequest();
            }
        });
    }

    public void executeKillOnAllNodes(KillJobsRequest request, final ActionListener<KillResponse> listener) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        final AtomicInteger counter = new AtomicInteger(nodes.size());
        final AtomicLong numKilled = new AtomicLong();
        final AtomicReference<Throwable> lastFailure = new AtomicReference<>();

        ActionListener<KillResponse> killResponseActionListener = new ActionListener<KillResponse>() {
            @Override
            public void onResponse(KillResponse killResponse) {
                numKilled.getAndAdd(killResponse.numKilled());
                countdown();
            }

            @Override
            public void onFailure(Throwable e) {
                lastFailure.set(e);
                countdown();
            }

            private void countdown() {
                if (counter.decrementAndGet() == 0) {
                    Throwable throwable = lastFailure.get();
                    if (throwable == null) {
                        listener.onResponse(new KillResponse(numKilled.get()));
                    } else {
                        listener.onFailure(throwable);
                    }
                }

            }
        };
        DefaultTransportResponseHandler<KillResponse> transportResponseHandler =
                new DefaultTransportResponseHandler<KillResponse>(killResponseActionListener) {
            @Override
            public KillResponse newInstance() {
                return new KillResponse(0);
            }
        };

        for (DiscoveryNode node : nodes) {
            transports.executeLocalOrWithTransport(
                    this, node.id(), request, killResponseActionListener, transportResponseHandler);
        }
    }

    @Override
    public String actionName() {
        return TRANSPORT_ACTION;
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    public void nodeOperation(KillJobsRequest request, ActionListener<KillResponse> listener) {
        try {
            listener.onResponse(new KillResponse(jobContextService.killJobs(request.toKill())));
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }
}
