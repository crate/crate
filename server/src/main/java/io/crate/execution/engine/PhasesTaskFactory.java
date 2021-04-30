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

import io.crate.execution.dsl.phases.NodeOperationTree;
import io.crate.execution.jobs.JobSetup;
import io.crate.execution.jobs.TasksService;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.transport.TransportJobAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

@Singleton
public final class PhasesTaskFactory {

    private final ClusterService clusterService;
    private final JobSetup jobSetup;
    private final TasksService tasksService;
    private final IndicesService indicesService;
    private final TransportJobAction jobAction;
    private final TransportKillJobsNodeAction killJobsNodeAction;
    private final Executor searchExecutor;

    @Inject
    public PhasesTaskFactory(ClusterService clusterService,
                             ThreadPool threadPool,
                             JobSetup jobSetup,
                             TasksService tasksService,
                             IndicesService indicesService,
                             TransportJobAction jobAction,
                             TransportKillJobsNodeAction killJobsNodeAction) {
        this.clusterService = clusterService;
        this.jobSetup = jobSetup;
        this.tasksService = tasksService;
        this.indicesService = indicesService;
        this.jobAction = jobAction;
        this.killJobsNodeAction = killJobsNodeAction;
        this.searchExecutor = threadPool.executor(ThreadPool.Names.SEARCH);
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
            killJobsNodeAction,
            nodeOperationTreeList,
            enableProfiling,
            searchExecutor
        );
    }
}
