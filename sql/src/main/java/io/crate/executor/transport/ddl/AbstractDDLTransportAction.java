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

package io.crate.executor.transport.ddl;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.AckedClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.function.Function;
import java.util.function.Supplier;

public abstract class AbstractDDLTransportAction<Request extends AcknowledgedRequest<Request>, Response extends AcknowledgedResponse> extends TransportMasterNodeAction<Request, Response> {

    private final Supplier<Response> responseSupplier;
    private final Function<Boolean, Response> ackedResponseFunction;
    private final String source;

    public AbstractDDLTransportAction(Settings settings,
                                      String actionName,
                                      TransportService transportService,
                                      ClusterService clusterService,
                                      ThreadPool threadPool,
                                      ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      Supplier<Request> requestSupplier,
                                      Supplier<Response> responseSupplier,
                                      Function<Boolean, Response> ackedResponseFunction,
                                      String source) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters,
            indexNameExpressionResolver, requestSupplier);
        this.responseSupplier = responseSupplier;
        this.ackedResponseFunction = ackedResponseFunction;
        this.source = source;
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected Response newResponse() {
        return responseSupplier.get();
    }

    @Override
    protected void masterOperation(Request request, ClusterState state, ActionListener<Response> listener) throws Exception {
        clusterService.submitStateUpdateTask(source,
            request,
            ClusterStateTaskConfig.build(Priority.HIGH, request.masterNodeTimeout()),
            clusterStateTaskExecutor(request),
            new AckedClusterStateTaskListener() {
                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public boolean mustAck(DiscoveryNode discoveryNode) {
                    return true;
                }

                @Override
                public void onAllNodesAcked(@Nullable Exception e) {
                    listener.onResponse(ackedResponseFunction.apply(true));
                }

                @Override
                public void onAckTimeout() {
                    listener.onResponse(ackedResponseFunction.apply(false));
                }

                @Override
                public TimeValue ackTimeout() {
                    return request.ackTimeout();
                }
            });

    }

    public abstract ClusterStateTaskExecutor<Request> clusterStateTaskExecutor(Request request);
}
