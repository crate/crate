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

package io.crate.planner;

import io.crate.action.sql.DCLStatementDispatcher;
import io.crate.action.sql.DDLStatementDispatcher;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.executor.transport.ddl.TransportDropTableAction;
import io.crate.executor.transport.executionphases.PhasesTaskFactory;
import io.crate.jobs.JobContextService;
import io.crate.metadata.Functions;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.sources.SystemCollectSource;
import io.crate.planner.projection.builder.ProjectionBuilder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ScheduledExecutorService;

/**
 * AKA Godzilla
 */
@Singleton
public class DependencyCarrier {

    private final Settings settings;
    private final TransportActionProvider transportActionProvider;
    private final PhasesTaskFactory phasesTaskFactory;
    private final ThreadPool threadPool;
    private final Functions functions;
    private final DDLStatementDispatcher ddlAnalysisDispatcherProvider;
    private final ClusterService clusterService;
    private final DCLStatementDispatcher dclStatementDispatcher;
    private final TransportDropTableAction transportDropTableAction;
    private final ProjectionBuilder projectionBuilder;

    @Inject
    public DependencyCarrier(Settings settings,
                             TransportActionProvider transportActionProvider,
                             PhasesTaskFactory phasesTaskFactory,
                             JobContextService jobContextService,
                             ThreadPool threadPool,
                             Functions functions,
                             DDLStatementDispatcher ddlAnalysisDispatcherProvider,
                             ClusterService clusterService,
                             NodeJobsCounter nodeJobsCounter,
                             SystemCollectSource systemCollectSource,
                             DCLStatementDispatcher dclStatementDispatcher,
                             TransportDropTableAction transportDropTableAction) {
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.phasesTaskFactory = phasesTaskFactory;
        this.threadPool = threadPool;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.clusterService = clusterService;
        this.dclStatementDispatcher = dclStatementDispatcher;
        this.transportDropTableAction = transportDropTableAction;
        projectionBuilder = new ProjectionBuilder(functions);
    }

    public DDLStatementDispatcher ddlAction() {
        return ddlAnalysisDispatcherProvider;
    }

    public DCLStatementDispatcher dclAction() {
        return dclStatementDispatcher;
    }

    public Functions functions() {
        return functions;
    }

    public TransportActionProvider transportActionProvider()  {
        return transportActionProvider;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    public ScheduledExecutorService scheduler() {
        return threadPool.scheduler();
    }

    public Settings settings() {
        return settings;
    }

    public ProjectionBuilder projectionBuilder() {
        return projectionBuilder;
    }

    public String localNodeId() {
        return clusterService().localNode().getId();
    }

    public TransportDropTableAction transportDropTableAction() {
        return transportDropTableAction;
    }

    public PhasesTaskFactory phasesTaskFactory() {
        return phasesTaskFactory;
    }
}
