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

package io.crate.execution.engine.collect.sources;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.ThreadPool;

import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.common.collections.Iterables;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.SentinelRow;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.FileUriCollectPhase;
import io.crate.execution.dsl.phases.ForeignCollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.phases.TableFunctionCollectPhase;
import io.crate.execution.engine.collect.CollectTask;
import io.crate.execution.engine.pipeline.ProjectionToProjectorVisitor;
import io.crate.execution.engine.pipeline.ProjectorFactory;
import io.crate.execution.jobs.NodeLimits;
import io.crate.expression.InputFactory;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.fdw.ForeignDataWrappers;
import io.crate.metadata.IndexParts;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.TableInfo;

@Singleton
public class CollectSourceResolver {

    private final CollectSource emptyCollectSource;
    private final Map<String, CollectSource> systemTableSource = new HashMap<>();
    private final ShardCollectSource shardCollectSource;
    private final CollectSource fileCollectSource;
    private final ClusterService clusterService;
    private final CollectPhaseVisitor visitor;
    private final ProjectorSetupCollectSource tableFunctionSource;
    private final CollectSource foreignDataWrappers;

    @Inject
    public CollectSourceResolver(ClusterService clusterService,
                                 NodeLimits nodeJobsCounter,
                                 CircuitBreakerService circuitBreakerService,
                                 NodeContext nodeCtx,
                                 Settings settings,
                                 ThreadPool threadPool,
                                 Node node,
                                 InformationSchemaInfo informationSchemaInfo,
                                 SysSchemaInfo sysSchemaInfo,
                                 PgCatalogSchemaInfo pgCatalogSchemaInfo,
                                 ShardCollectSource shardCollectSource,
                                 FileCollectSource fileCollectSource,
                                 TableFunctionCollectSource tableFunctionCollectSource,
                                 SystemCollectSource systemCollectSource,
                                 NodeStatsCollectSource nodeStatsCollectSource,
                                 ForeignDataWrappers foreignDataWrappers) {
        this.clusterService = clusterService;

        EvaluatingNormalizer normalizer = EvaluatingNormalizer.functionOnlyNormalizer(nodeCtx);
        ProjectorFactory projectorFactory = new ProjectionToProjectorVisitor(
            clusterService,
            nodeJobsCounter,
            circuitBreakerService,
            nodeCtx,
            threadPool,
            settings,
            node.client(),
            new InputFactory(nodeCtx),
            normalizer,
            systemCollectSource::getRowUpdater,
            systemCollectSource::tableDefinition
        );
        this.shardCollectSource = shardCollectSource;
        this.fileCollectSource = new ProjectorSetupCollectSource(fileCollectSource, projectorFactory);
        this.tableFunctionSource = new ProjectorSetupCollectSource(tableFunctionCollectSource, projectorFactory);
        this.emptyCollectSource = new ProjectorSetupCollectSource(new VoidCollectSource(), projectorFactory);
        this.foreignDataWrappers = new ProjectorSetupCollectSource(foreignDataWrappers, projectorFactory);

        ProjectorSetupCollectSource sysSource = new ProjectorSetupCollectSource(systemCollectSource, projectorFactory);
        for (TableInfo tableInfo : sysSchemaInfo.getTables()) {
            systemTableSource.put(tableInfo.ident().fqn(), sysSource);
        }
        systemTableSource.put(SysNodesTableInfo.IDENT.fqn(), new ProjectorSetupCollectSource(nodeStatsCollectSource, projectorFactory));

        for (TableInfo tableInfo : pgCatalogSchemaInfo.getTables()) {
            systemTableSource.put(tableInfo.ident().fqn(), sysSource);
        }
        for (TableInfo tableInfo : informationSchemaInfo.getTables()) {
            systemTableSource.put(tableInfo.ident().fqn(), sysSource);
        }

        visitor = new CollectPhaseVisitor();
    }

    private class CollectPhaseVisitor extends ExecutionPhaseVisitor<Void, CollectSource> {

        @Override
        public CollectSource visitFileUriCollectPhase(FileUriCollectPhase phase, Void context) {
            return fileCollectSource;
        }

        @Override
        public CollectSource visitTableFunctionCollect(TableFunctionCollectPhase phase, Void context) {
            return tableFunctionSource;
        }

        @Override
        public CollectSource visitForeignCollect(ForeignCollectPhase phase, Void context) {
            return foreignDataWrappers;
        }

        @Override
        public CollectSource visitRoutedCollectPhase(RoutedCollectPhase phase, Void context) {
            if (!phase.isRouted()) {
                return emptyCollectSource;
            }
            String localNodeId = clusterService.state().nodes().getLocalNodeId();
            Set<String> routingNodes = phase.routing().nodes();
            if (!routingNodes.contains(localNodeId)) {
                throw new IllegalStateException("unsupported routing");
            }

            if (phase.routing().containsShards(localNodeId)) {
                return shardCollectSource;
            }

            Map<String, Map<String, IntIndexedContainer>> locations = phase.routing().locations();
            Map<String, IntIndexedContainer> indexShards = locations.get(localNodeId);
            if (indexShards == null) {
                throw new IllegalStateException("Can't resolve CollectService for collectPhase: " + phase);
            }

            String indexName = Iterables.getFirst(indexShards.keySet(), null);
            if (indexName == null) {
                throw new IllegalStateException("Can't resolve CollectService for collectPhase: " + phase);
            }
            if (phase.maxRowGranularity() == RowGranularity.DOC && IndexParts.isPartitioned(indexName)) {
                // partitioned table without any shards; nothing to collect
                return emptyCollectSource;
            }
            assert indexShards.size() ==
                   1 : "routing without shards that operates on non user-tables may only contain 1 index/table";
            CollectSource collectSource = systemTableSource.get(indexName);
            if (collectSource == null) {
                throw new IllegalStateException("Can't resolve CollectService for collectPhase: " + phase);
            }
            return collectSource;
        }
    }

    public CollectSource getService(CollectPhase collectPhase) {
        return collectPhase.accept(visitor, null);
    }

    private static class VoidCollectSource implements CollectSource {

        @Override
        public CompletableFuture<BatchIterator<Row>> getIterator(TransactionContext txnCtx,
                                                                 CollectPhase collectPhase,
                                                                 CollectTask collectTask,
                                                                 boolean supportMoveToStart) {
            return CompletableFuture.completedFuture(InMemoryBatchIterator.empty(SentinelRow.SENTINEL));
        }
    }
}
