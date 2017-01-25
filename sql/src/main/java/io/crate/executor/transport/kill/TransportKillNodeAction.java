/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.executor.transport.kill;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.MultiActionListener;
import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.jobs.JobContextService;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

abstract class TransportKillNodeAction<Request extends TransportRequest> extends AbstractComponent
    implements NodeAction<Request, KillResponse>, Callable<Request> {

    protected final JobContextService jobContextService;
    protected final ClusterService clusterService;
    protected final TransportService transportService;
    protected final String name;

    TransportKillNodeAction(String name,
                            Settings settings,
                            JobContextService jobContextService,
                            ClusterService clusterService,
                            TransportService transportService,
                            Supplier<Request> requestSupplier) {
        super(settings);
        this.jobContextService = jobContextService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.name = name;
        transportService.registerRequestHandler(
            name,
            requestSupplier,
            ThreadPool.Names.GENERIC,
            new NodeActionRequestHandler<Request, KillResponse>(this) {});
    }

    protected abstract ListenableFuture<Integer> doKill(Request request);

    @Override
    public void nodeOperation(Request request, final ActionListener<KillResponse> listener) {
        Futures.addCallback(doKill(request), new FutureCallback<Integer>() {
            @Override
            public void onSuccess(@Nullable Integer result) {
                assert result != null : "result must not be null";
                listener.onResponse(new KillResponse(result));
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                listener.onFailure((Exception) t);
            }
        });
    }

    /**
     * Broadcasts the given kill request to all nodes in the cluster
     */
    public void broadcast(Request request, ActionListener<KillResponse> listener) {
        broadcast(request, listener, Collections.emptyList());
    }

    public void broadcast(Request request, ActionListener<KillResponse> listener, Collection<String> excludedNodeIds) {
        Stream<DiscoveryNode> nodes = StreamSupport.stream(clusterService.state().nodes().spliterator(), false);
        Collection<DiscoveryNode> filteredNodes = nodes.filter(node -> !excludedNodeIds.contains(node.getId())).collect(Collectors.toList());

        listener = new MultiActionListener<>(filteredNodes.size(), KillResponse.MERGE_FUNCTION, listener);
        DefaultTransportResponseHandler<KillResponse> responseHandler =
            new DefaultTransportResponseHandler<KillResponse>(listener) {

                @Override
                public KillResponse newInstance() {
                    return new KillResponse(0);
                }
            };
        for (DiscoveryNode node : filteredNodes) {
            transportService.sendRequest(node, name, request, responseHandler);
        }
    }
}
