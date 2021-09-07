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

package io.crate.planner;

import io.crate.action.sql.DCLStatementDispatcher;
import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.execution.TransportActionProvider;
import io.crate.execution.ddl.RepositoryService;
import io.crate.execution.ddl.TransportSwapRelationsAction;
import io.crate.execution.ddl.tables.AlterTableOperation;
import io.crate.execution.ddl.tables.TransportDropTableAction;
import io.crate.execution.ddl.views.TransportCreateViewAction;
import io.crate.execution.ddl.views.TransportDropViewAction;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.PhasesTaskFactory;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.udf.TransportCreateUserDefinedFunctionAction;
import io.crate.expression.udf.TransportDropUserDefinedFunctionAction;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Schemas;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.replication.logical.action.TransportCreatePublicationAction;
import io.crate.replication.logical.action.TransportCreateSubscriptionAction;
import io.crate.replication.logical.action.TransportDropPublicationAction;
import io.crate.statistics.TransportAnalyzeAction;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
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
    private final NodeContext nodeCtx;
    private final ClusterService clusterService;
    private final DCLStatementDispatcher dclStatementDispatcher;
    private final TransportDropTableAction transportDropTableAction;
    private final ProjectionBuilder projectionBuilder;
    private final TransportCreateViewAction createViewAction;
    private final TransportDropViewAction dropViewAction;
    private final TransportSwapRelationsAction swapRelationsAction;
    private final TransportCreateIndexAction createIndexAction;
    private final TransportCreateUserDefinedFunctionAction createFunctionAction;
    private final TransportDropUserDefinedFunctionAction dropFunctionAction;
    private final Provider<TransportAnalyzeAction> analyzeAction;
    private final AlterTableOperation alterTableOperation;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final RepositoryService repositoryService;
    private final RepositoryParamValidator repositoryParamValidator;
    private final NodeLimits nodeLimits;
    private final TransportCreatePublicationAction createPublicationAction;
    private final TransportDropPublicationAction dropPublicationAction;
    private final TransportCreateSubscriptionAction createSubscriptionAction;
    private final LogicalReplicationService logicalReplicationService;

    @Inject
    public DependencyCarrier(Settings settings,
                             TransportActionProvider transportActionProvider,
                             PhasesTaskFactory phasesTaskFactory,
                             ThreadPool threadPool,
                             Schemas schemas,
                             NodeContext nodeCtx,
                             ClusterService clusterService,
                             NodeLimits nodeLimits,
                             DCLStatementDispatcher dclStatementDispatcher,
                             TransportDropTableAction transportDropTableAction,
                             TransportCreateViewAction createViewAction,
                             TransportDropViewAction dropViewAction,
                             TransportSwapRelationsAction swapRelationsAction,
                             TransportCreateIndexAction createIndexAction,
                             TransportCreateUserDefinedFunctionAction createFunctionAction,
                             TransportDropUserDefinedFunctionAction dropFunctionAction,
                             Provider<TransportAnalyzeAction> analyzeAction,
                             AlterTableOperation alterTableOperation,
                             FulltextAnalyzerResolver fulltextAnalyzerResolver,
                             RepositoryService repositoryService,
                             RepositoryParamValidator repositoryParamValidator,
                             TransportCreatePublicationAction createPublicationAction,
                             TransportDropPublicationAction dropPublicationAction,
                             TransportCreateSubscriptionAction createSubscriptionAction,
                             LogicalReplicationService logicalReplicationService) {
        this.settings = settings;
        this.transportActionProvider = transportActionProvider;
        this.phasesTaskFactory = phasesTaskFactory;
        this.threadPool = threadPool;
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
        this.clusterService = clusterService;
        this.nodeLimits = nodeLimits;
        this.dclStatementDispatcher = dclStatementDispatcher;
        this.transportDropTableAction = transportDropTableAction;
        projectionBuilder = new ProjectionBuilder(nodeCtx);
        this.createViewAction = createViewAction;
        this.dropViewAction = dropViewAction;
        this.swapRelationsAction = swapRelationsAction;
        this.createIndexAction = createIndexAction;
        this.createFunctionAction = createFunctionAction;
        this.dropFunctionAction = dropFunctionAction;
        this.analyzeAction = analyzeAction;
        this.alterTableOperation = alterTableOperation;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.repositoryService = repositoryService;
        this.repositoryParamValidator = repositoryParamValidator;
        this.createPublicationAction = createPublicationAction;
        this.dropPublicationAction = dropPublicationAction;
        this.createSubscriptionAction = createSubscriptionAction;
        this.logicalReplicationService = logicalReplicationService;
    }

    public Schemas schemas() {
        return schemas;
    }

    public TransportSwapRelationsAction swapRelationsAction() {
        return swapRelationsAction;
    }

    public DCLStatementDispatcher dclAction() {
        return dclStatementDispatcher;
    }

    public NodeContext nodeContext() {
        return nodeCtx;
    }

    public TransportActionProvider transportActionProvider() {
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

    public ThreadPool threadPool() {
        return threadPool;
    }

    public TransportCreateViewAction createViewAction() {
        return createViewAction;
    }

    public TransportDropViewAction dropViewAction() {
        return dropViewAction;
    }

    public TransportCreateIndexAction createIndexAction() {
        return createIndexAction;
    }

    public TransportCreateUserDefinedFunctionAction createFunctionAction() {
        return createFunctionAction;
    }

    public TransportDropUserDefinedFunctionAction dropFunctionAction() {
        return dropFunctionAction;
    }

    public FulltextAnalyzerResolver fulltextAnalyzerResolver() {
        return fulltextAnalyzerResolver;
    }

    public AlterTableOperation alterTableOperation() {
        return alterTableOperation;
    }

    public RepositoryParamValidator repositoryParamValidator() {
        return repositoryParamValidator;
    }

    public RepositoryService repositoryService() {
        return repositoryService;
    }

    public TransportAnalyzeAction analyzeAction() {
        return analyzeAction.get();
    }

    public NodeLimits nodeLimits() {
        return nodeLimits;
    }

    public TransportCreatePublicationAction createPublicationAction() {
        return createPublicationAction;
    }

    public TransportDropPublicationAction dropPublicationAction() {
        return dropPublicationAction;
    }

    public TransportCreateSubscriptionAction createSubscriptionAction() {
        return createSubscriptionAction;
    }

    public LogicalReplicationService logicalReplicationService() {
        return logicalReplicationService;
    }
}
