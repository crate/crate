/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine;

import java.util.List;
import java.util.UUID;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.Node;

import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.jobs.JobSetup;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.KillJobsNodeAction;
import io.crate.execution.jobs.kill.KillJobsNodeRequest;
import io.crate.execution.jobs.kill.KillResponse;
import io.crate.execution.jobs.transport.JobAction;
import io.crate.execution.jobs.transport.JobRequest;
import io.crate.execution.jobs.transport.JobResponse;
import io.crate.execution.support.ActionExecutor;
import io.crate.execution.support.NodeRequest;

@Singleton
public final class PhasesTaskFactory {

    private final ClusterService clusterService;
    private final JobSetup jobSetup;
    private final TasksService tasksService;
    private final IndicesService indicesService;
    private final ActionExecutor<NodeRequest<JobRequest>, JobResponse> jobAction;
    private final ActionExecutor<KillJobsNodeRequest, KillResponse> killNodeAction;

    @Inject
    public PhasesTaskFactory(ClusterService clusterService,
                             JobSetup jobSetup,
                             TasksService tasksService,
                             IndicesService indicesService,
                             Node node) {
        this.clusterService = clusterService;
        this.jobSetup = jobSetup;
        this.tasksService = tasksService;
        this.indicesService = indicesService;
        this.jobAction = req -> node.client().execute(JobAction.INSTANCE, req);
        this.killNodeAction = req -> node.client().execute(KillJobsNodeAction.INSTANCE, req);
    }

    public JobLauncher create(UUID jobId, List<NodeOperationTree> nodeOperationTreeList) {
        return create(jobId, nodeOperationTreeList, false);
    }

    public JobLauncher create(UUID jobId, List<NodeOperationTree> nodeOperationTreeList, boolean enableProfiling) {
        return new JobLauncher(
            jobId,
            clusterService,
            jobSetup,
            tasksService,
            indicesService,
            jobAction,
            killNodeAction,
            nodeOperationTreeList,
            enableProfiling
        );
    }
}
