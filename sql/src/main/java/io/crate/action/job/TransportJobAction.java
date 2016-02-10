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

package io.crate.action.job;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.jobs.KeepAliveTimers;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import java.util.List;

@Singleton
public class TransportJobAction implements NodeAction<JobRequest, JobResponse> {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportJobAction.class);

    public static final String ACTION_NAME = "crate/sql/job";
    private static final String EXECUTOR = ThreadPool.Names.PERCOLATE;

    private final IndicesService indicesService;
    private final Transports transports;
    private final JobContextService jobContextService;
    private final ContextPreparer contextPreparer;
    private final KeepAliveTimers keepAliveTimers;
    private final ClusterService clusterService;

    @Inject
    public TransportJobAction(TransportService transportService,
                              IndicesService indicesService,
                              ClusterService clusterService,
                              Transports transports,
                              JobContextService jobContextService,
                              ContextPreparer contextPreparer,
                              KeepAliveTimers keepAliveTimers) {
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transports = transports;
        this.jobContextService = jobContextService;
        this.contextPreparer = contextPreparer;
        this.keepAliveTimers = keepAliveTimers;

        transportService.registerRequestHandler(ACTION_NAME,
                JobRequest.class,
                ThreadPool.Names.GENERIC,
                new NodeActionRequestHandler<JobRequest, JobResponse>(this) { });
    }

    public void execute(String node, final JobRequest request, final ActionListener<JobResponse> listener) {
        transports.executeLocalOrWithTransport(this, node, request, listener,
                new DefaultTransportResponseHandler<JobResponse>(listener) {
                    @Override
                    public JobResponse newInstance() {
                        return new JobResponse();
                    }
                });
    }

    @Override
    public void nodeOperation(final JobRequest request, final ActionListener<JobResponse> actionListener) {
        JobExecutionContext.Builder contextBuilder = jobContextService.newBuilder(request.jobId());

        SharedShardContexts sharedShardContexts = new SharedShardContexts(indicesService);
        List<ListenableFuture<Bucket>> directResponseFutures = contextPreparer.prepareOnRemote(
                request.jobId(), request.nodeOperations(), contextBuilder, sharedShardContexts);

        try {
            JobExecutionContext context = jobContextService.createContext(contextBuilder);
            context.start();
        } catch (Throwable t){
            actionListener.onFailure(t);
            return;
        }

        if (directResponseFutures.size() == 0) {
            actionListener.onResponse(new JobResponse());
        } else {
            String nodeId = nodeId(request.remoteAddress());
            final KeepAliveTimers.ResettableTimer keepAliveTimer;
            if (nodeId == null) {
                // assuming local node in this case
                LOGGER.debug("Could not determine direct response node for address [{}]. Assuming local node. Not starting keep alive timer.", request.remoteAddress());
                keepAliveTimer = null;
            } else {
                keepAliveTimer = keepAliveTimers.forJobOnNode(request.jobId(), nodeId);
                keepAliveTimer.start();
            }
            Futures.addCallback(Futures.allAsList(directResponseFutures), new FutureCallback<List<Bucket>>() {
                @Override
                public void onSuccess(List<Bucket> buckets) {
                    if (keepAliveTimer != null) {
                        keepAliveTimer.cancel();
                    }
                    actionListener.onResponse(new JobResponse(buckets));
                }

                @Override
                public void onFailure(@Nonnull Throwable t) {
                    if (keepAliveTimer != null) {
                        keepAliveTimer.cancel();
                    }
                    actionListener.onFailure(t);
                }
            });
        }
    }

    @Override
    public String actionName() {
        return ACTION_NAME;
    }

    @Override
    public String executorName() {
        return EXECUTOR;
    }

    @Nullable
    private String nodeId(TransportAddress transportAddress) {
        String nodeId = null;
        DiscoveryNode discoveryNode = clusterService.state().nodes().findByAddress(transportAddress);
        if (discoveryNode != null) {
            nodeId = discoveryNode.id();
        }
        return nodeId;
    }
}
