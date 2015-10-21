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

package io.crate.action.job;

import io.crate.executor.transport.DefaultTransportResponseHandler;
import io.crate.executor.transport.NodeAction;
import io.crate.executor.transport.NodeActionRequestHandler;
import io.crate.executor.transport.Transports;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportKeepAliveAction implements NodeAction<KeepAliveRequest, TransportResponse.Empty> {

    private static final ESLogger LOGGER = Loggers.getLogger(TransportKeepAliveAction.class);
    public static final String ACTION_NAME = "crate/sql/job/keep_alive";
    private static final String EXECUTOR = ThreadPool.Names.SAME;

    private final JobContextService jobContextService;
    private final Transports transports;

    @Inject
    public TransportKeepAliveAction(TransportService transportService,
                                    Transports transports,
                                    JobContextService jobContextService) {
        this.jobContextService = jobContextService;
        this.transports = transports;
        transportService.registerHandler(ACTION_NAME, new NodeActionRequestHandler<KeepAliveRequest, TransportResponse.Empty>(this) {
            @Override
            public KeepAliveRequest newInstance() {
                return new KeepAliveRequest();
            }
        });
    }

    public void keepAlive(String node, final KeepAliveRequest request, final ActionListener<TransportResponse.Empty> listener) {
        transports.executeLocalOrWithTransport(this, node, request, listener, new DefaultTransportResponseHandler<TransportResponse.Empty>(listener) {
            @Override
            public TransportResponse.Empty newInstance() {
                return TransportResponse.Empty.INSTANCE;
            }
        });
    }

    @Override
    public String actionName() {
        return ACTION_NAME;
    }

    @Override
    public String executorName() {
        return EXECUTOR;
    }

    @Override
    public void nodeOperation(KeepAliveRequest request, ActionListener<TransportResponse.Empty> listener) {
        JobExecutionContext context = jobContextService.getContextOrNull(request.jobId());
        if (context != null) {
            LOGGER.trace("keeping JobExecutionContext {} alive ", context);
            context.externalKeepAlive();
        } else {
            LOGGER.trace("no JobExecutionContext found for jobId {}", request.jobId());
        }
        listener.onResponse(TransportResponse.Empty.INSTANCE);
    }

}
