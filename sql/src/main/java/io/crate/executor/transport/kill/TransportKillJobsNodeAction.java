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

import io.crate.jobs.JobContextService;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CompletableFuture;

@Singleton
public class TransportKillJobsNodeAction extends TransportKillNodeAction<KillJobsRequest> {

    @Inject
    public TransportKillJobsNodeAction(Settings settings,
                                       JobContextService jobContextService,
                                       ClusterService clusterService,
                                       TransportService transportService) {
        super("crate/sql/kill_jobs", settings, jobContextService, clusterService, transportService);
    }

    @Override
    protected CompletableFuture<Integer> doKill(KillJobsRequest request) {
        return jobContextService.killJobs(request.toKill());
    }

    @Override
    public KillJobsRequest call() throws Exception {
        return new KillJobsRequest();
    }
}
