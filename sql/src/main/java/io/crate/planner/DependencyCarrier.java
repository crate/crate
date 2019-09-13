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
import io.crate.execution.TransportActionProvider;
import io.crate.execution.ddl.DDLStatementDispatcher;
import io.crate.execution.ddl.TransportSwapRelationsAction;
import io.crate.execution.ddl.tables.AlterTableOperation;
import io.crate.execution.ddl.tables.TransportDropTableAction;
import io.crate.execution.ddl.views.TransportCreateViewAction;
import io.crate.execution.ddl.views.TransportDropViewAction;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.PhasesTaskFactory;
import io.crate.license.LicenseService;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.Functions;
import io.crate.metadata.Schemas;
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
    private final Schemas schemas;
    private final Functions functions;
    private final DDLStatementDispatcher ddlAnalysisDispatcherProvider;
    private final ClusterService clusterService;
    private final DCLStatementDispatcher dclStatementDispatcher;
    private final TransportDropTableAction transportDropTableAction;
    private final ProjectionBuilder projectionBuilder;
    private final TransportCreateViewAction createViewAction;
    private final TransportDropViewAction dropViewAction;
    private final TransportSwapRelationsAction swapRelationsAction;
    private final LicenseService licenseService;
    private final AlterTableOperation alterTableOperation;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;

    @Inject
    public DependencyCarrier(Settings settings,
                             TransportActionProvider transportActionProvider,
                             PhasesTaskFactory phasesTaskFactory,
                             ThreadPool threadPool,
                             Schemas schemas,
                             Functions functions,
                             DDLStatementDispatcher ddlAnalysisDispatcherProvider,
                             ClusterService clusterService,
                             LicenseService licenseService,
                             DCLStatementDispatcher dclStatementDispatcher,
                             TransportDropTableAction transportDropTableAction,
                             TransportCreateViewAction createViewAction,
                             TransportDropViewAction dropViewAction,
                             TransportSwapRelationsAction swapRelationsAction,
                             AlterTableOperation alterTableOperation,
                             FulltextAnalyzerResolver fulltextAnalyzerResolver) {
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.phasesTaskFactory = phasesTaskFactory;
        this.threadPool = threadPool;
        this.schemas = schemas;
        this.functions = functions;
        this.ddlAnalysisDispatcherProvider = ddlAnalysisDispatcherProvider;
        this.clusterService = clusterService;
        this.licenseService = licenseService;
        this.dclStatementDispatcher = dclStatementDispatcher;
        this.transportDropTableAction = transportDropTableAction;
        projectionBuilder = new ProjectionBuilder(functions);
        this.createViewAction = createViewAction;
        this.dropViewAction = dropViewAction;
        this.swapRelationsAction = swapRelationsAction;
        this.alterTableOperation = alterTableOperation;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
    }

    public Schemas schemas() {
        return schemas;
    }

    public TransportSwapRelationsAction swapRelationsAction() {
        return swapRelationsAction;
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

    public TransportActionProvider transportActionProvider() {
        return transportActionProvider;
    }

    public ClusterService clusterService() {
        return clusterService;
    }

    public LicenseService licenseService() {
        return licenseService;
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

    public ThreadPool threadPool() {
        return threadPool;
    }

    public TransportCreateViewAction createViewAction() {
        return createViewAction;
    }

    public TransportDropViewAction dropViewAction() {
        return dropViewAction;
    }

    public FulltextAnalyzerResolver fulltextAnalyzerResolver() {
        return fulltextAnalyzerResolver;
    }

    public AlterTableOperation alterTableOperation() {
        return alterTableOperation;
    }
}
