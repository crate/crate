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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

@Singleton
public class TransportKillJobsNodeAction implements NodeAction<KillJobsRequest, KillResponse> {

    private static final String TRANSPORT_ACTION = "crate/sql/kill_jobs";

    private JobContextService jobContextService;
    private Transports transports;

    @Inject
    public TransportKillJobsNodeAction(JobContextService jobContextService,
                                       Transports transports,
                                       TransportService transportService) {
        this.jobContextService = jobContextService;
        this.transports = transports;
        transportService.registerHandler(TRANSPORT_ACTION, new NodeActionRequestHandler<KillJobsRequest, KillResponse>(this) {
            @Override
            public KillJobsRequest newInstance() {
                return new KillJobsRequest();
            }
        });
    }

    public void execute(String targetNode, KillJobsRequest request, ActionListener<KillResponse> listener) {
        transports.executeLocalOrWithTransport(this, targetNode, request, listener,
                new DefaultTransportResponseHandler<KillResponse>(listener) {
                    @Override
                    public KillResponse newInstance() {
                        return new KillResponse(0);
                    }
                });
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
        listener.onResponse(new KillResponse(jobContextService.killJobs(request.toKill())));
    }
}
