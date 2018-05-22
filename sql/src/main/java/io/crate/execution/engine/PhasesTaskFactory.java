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

package io.crate.execution.engine;

import io.crate.execution.jobs.ContextPreparer;
import io.crate.execution.jobs.transport.TransportJobAction;
import io.crate.execution.jobs.kill.TransportKillJobsNodeAction;
import io.crate.execution.jobs.JobContextService;
import io.crate.execution.dsl.phases.NodeOperationTree;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.indices.IndicesService;

import java.util.List;
import java.util.UUID;

@Singleton
public final class PhasesTaskFactory {

    private final ClusterService clusterService;
    private final ContextPreparer contextPreparer;
    private final JobContextService jobContextService;
    private final IndicesService indicesService;
    private final TransportJobAction jobAction;
    private final TransportKillJobsNodeAction killJobsNodeAction;

    @Inject
    public PhasesTaskFactory(ClusterService clusterService,
                             ContextPreparer contextPreparer,
                             JobContextService jobContextService,
                             IndicesService indicesService,
                             TransportJobAction jobAction,
                             TransportKillJobsNodeAction killJobsNodeAction) {
        this.clusterService = clusterService;
        this.contextPreparer = contextPreparer;
        this.jobContextService = jobContextService;
        this.indicesService = indicesService;
        this.jobAction = jobAction;
        this.killJobsNodeAction = killJobsNodeAction;
    }

    public ExecutionPhasesTask create(UUID jobId, List<NodeOperationTree> nodeOperationTreeList) {
        return create(jobId, nodeOperationTreeList, false);
    }

    public ExecutionPhasesTask create(UUID jobId, List<NodeOperationTree> nodeOperationTreeList, boolean enableProfiling) {
        return new ExecutionPhasesTask(
            jobId,
            clusterService,
            contextPreparer,
            jobContextService,
            indicesService,
            jobAction,
            killJobsNodeAction,
            nodeOperationTreeList,
            enableProfiling
        );
    }
}
