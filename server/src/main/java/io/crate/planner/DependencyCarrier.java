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

import java.util.concurrent.ScheduledExecutorService;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import io.crate.analyze.repositories.RepositoryParamValidator;
import io.crate.execution.ddl.RepositoryService;
import io.crate.execution.ddl.tables.AlterTableClient;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.execution.engine.PhasesTaskFactory;
import io.crate.execution.jobs.NodeLimits;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Schemas;
import io.crate.replication.logical.LogicalReplicationService;
import io.crate.session.DCLStatementDispatcher;
import io.crate.statistics.TransportAnalyzeAction;

/**
 * AKA Godzilla
 */
@Singleton
public class DependencyCarrier {

    private final Settings settings;
    private final PhasesTaskFactory phasesTaskFactory;
    private final ThreadPool threadPool;
    private final Schemas schemas;
    private final NodeContext nodeCtx;
    private final ClusterService clusterService;
    private final DCLStatementDispatcher dclStatementDispatcher;
    private final ProjectionBuilder projectionBuilder;
    private final Provider<TransportAnalyzeAction> analyzeAction;
    private final AlterTableClient alterTableClient;
    private final FulltextAnalyzerResolver fulltextAnalyzerResolver;
    private final RepositoryService repositoryService;
    private final RepositoryParamValidator repositoryParamValidator;
    private final NodeLimits nodeLimits;
    private final LogicalReplicationService logicalReplicationService;
    private final Client client;
    private final CircuitBreakerService circuitBreakerService;

    @Inject
    public DependencyCarrier(Settings settings,
                             Node node,
                             PhasesTaskFactory phasesTaskFactory,
                             ThreadPool threadPool,
                             NodeContext nodeCtx,
                             ClusterService clusterService,
                             NodeLimits nodeLimits,
                             CircuitBreakerService circuitBreakerService,
                             DCLStatementDispatcher dclStatementDispatcher,
                             Provider<TransportAnalyzeAction> analyzeAction,
                             AlterTableClient alterTableOperation,
                             FulltextAnalyzerResolver fulltextAnalyzerResolver,
                             RepositoryService repositoryService,
                             RepositoryParamValidator repositoryParamValidator,
                             LogicalReplicationService logicalReplicationService) {
        this.settings = settings;
        this.client = node.client();
        this.phasesTaskFactory = phasesTaskFactory;
        this.threadPool = threadPool;
        this.nodeCtx = nodeCtx;
        this.schemas = nodeCtx.schemas();
        this.clusterService = clusterService;
        this.nodeLimits = nodeLimits;
        this.circuitBreakerService = circuitBreakerService;
        this.dclStatementDispatcher = dclStatementDispatcher;
        projectionBuilder = new ProjectionBuilder(nodeCtx);
        this.analyzeAction = analyzeAction;
        this.alterTableClient = alterTableOperation;
        this.fulltextAnalyzerResolver = fulltextAnalyzerResolver;
        this.repositoryService = repositoryService;
        this.repositoryParamValidator = repositoryParamValidator;
        this.logicalReplicationService = logicalReplicationService;
    }

    public Schemas schemas() {
        return schemas;
    }

    public DCLStatementDispatcher dclAction() {
        return dclStatementDispatcher;
    }

    public NodeContext nodeContext() {
        return nodeCtx;
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

    public PhasesTaskFactory phasesTaskFactory() {
        return phasesTaskFactory;
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    public FulltextAnalyzerResolver fulltextAnalyzerResolver() {
        return fulltextAnalyzerResolver;
    }

    public AlterTableClient alterTableClient() {
        return alterTableClient;
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

    public LogicalReplicationService logicalReplicationService() {
        return logicalReplicationService;
    }

    public Client client() {
        return client;
    }

    public CircuitBreaker circuitBreaker(String name) {
        return circuitBreakerService.getBreaker(name);
    }
}
