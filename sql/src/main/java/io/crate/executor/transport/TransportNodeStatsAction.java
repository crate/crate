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

package io.crate.executor.transport;

import io.crate.operation.reference.sys.node.NodeStatsContext;
import io.crate.operation.reference.sys.node.NodeStatsContextFieldResolver;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportNodeStatsAction implements NodeAction<NodeStatsRequest, NodeStatsResponse> {

    private static final String ACTION_NAME = "crate/sql/sys/nodes";
    private static final String EXECUTOR = ThreadPool.Names.MANAGEMENT;

    private final NodeStatsContextFieldResolver nodeContextFieldsResolver;
    private final Transports transports;

    @Inject
    public TransportNodeStatsAction(TransportService transportService,
                                    NodeStatsContextFieldResolver nodeContextFieldsResolver,
                                    Transports transports) {
        this.nodeContextFieldsResolver = nodeContextFieldsResolver;
        this.transports = transports;
        transportService.registerRequestHandler(ACTION_NAME,
            NodeStatsRequest::new,
            EXECUTOR,
            new NodeActionRequestHandler<NodeStatsRequest, NodeStatsResponse>(this) {
            }
        );
    }

    public void execute(final String nodeName,
                        final NodeStatsRequest request,
                        final ActionListener<NodeStatsResponse> listener,
                        final TimeValue timeout) {
        TransportRequestOptions options = TransportRequestOptions.builder()
            .withTimeout(timeout)
            .build();

        transports.sendRequest(ACTION_NAME, nodeName, request, listener,
            new DefaultTransportResponseHandler<NodeStatsResponse>(listener) {
                @Override
                public NodeStatsResponse newInstance() {
                    return new NodeStatsResponse();
                }
            }, options);
    }

    @Override
    public void nodeOperation(NodeStatsRequest request, ActionListener<NodeStatsResponse> listener) {
        try {
            NodeStatsContext context = nodeContextFieldsResolver.forTopColumnIdents(request.columnIdents());
            listener.onResponse(new NodeStatsResponse(context));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
