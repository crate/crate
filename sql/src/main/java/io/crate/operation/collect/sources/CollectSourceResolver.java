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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.RowDownstream;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ProjectorFactory;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.FileUriCollectPhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.*;

@Singleton
public class CollectSourceResolver {

    private static final VoidCollectSource VOID_COLLECT_SERVICE = new VoidCollectSource();

    private final Map<String, CollectSource> nodeDocCollectSources = new HashMap<>();
    private final ShardCollectSource shardCollectSource;
    private final CollectSource fileCollectSource;
    private final CollectSource singleRowSource;

    @Inject
    @SuppressWarnings("ForLoopReplaceableByForEach")
    public CollectSourceResolver(ClusterService clusterService,
                                 Functions functions,
                                 ReferenceResolver clusterReferenceResolver,
                                 Settings settings,
                                 ThreadPool threadPool,
                                 TransportActionProvider transportActionProvider,
                                 BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                 InformationSchemaInfo informationSchemaInfo,
                                 SysSchemaInfo sysSchemaInfo,
                                 ShardCollectSource shardCollectSource,
                                 FileCollectSource fileCollectSource,
                                 SingleRowSource singleRowSource,
                                 InformationSchemaCollectSource informationSchemaCollectSource,
                                 SystemCollectSource systemCollectSource) {

        ImplementationSymbolVisitor nodeImplementationSymbolVisitor = new ImplementationSymbolVisitor(
                clusterReferenceResolver,
                functions,
                RowGranularity.NODE
        );
        ProjectorFactory projectorFactory = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                nodeImplementationSymbolVisitor
        );
        this.shardCollectSource = shardCollectSource;
        this.fileCollectSource = new ProjectorSetupCollectSource(fileCollectSource, projectorFactory);
        this.singleRowSource = new ProjectorSetupCollectSource(singleRowSource, projectorFactory);

        nodeDocCollectSources.put(SysClusterTableInfo.IDENT.fqn(), this.singleRowSource);

        ProjectorSetupCollectSource isSource = new ProjectorSetupCollectSource(informationSchemaCollectSource, projectorFactory);
        for (Iterator<? extends TableInfo> iterator = informationSchemaInfo.iterator(); iterator.hasNext(); ) {
            TableInfo tableInfo = iterator.next();
            nodeDocCollectSources.put(tableInfo.ident().fqn(), isSource);
        }

        ProjectorSetupCollectSource sysSource = new ProjectorSetupCollectSource(systemCollectSource, projectorFactory);
        for (Iterator<? extends TableInfo> iterator = sysSchemaInfo.iterator(); iterator.hasNext(); ) {
            TableInfo tableInfo = iterator.next();
            if (tableInfo.rowGranularity().equals(RowGranularity.DOC)) {
                nodeDocCollectSources.put(tableInfo.ident().fqn(), sysSource);
            }
        }
    }

    public CollectSource getService(CollectPhase collectPhase, String localNodeId) {

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        if (locations == null) {
            throw new IllegalArgumentException("routing of collectPhase must contain locations");
        }
        if (collectPhase.routing().containsShards(localNodeId) || collectPhase.routing().containsShards(TableInfo.NULL_NODE_ID)) {
            return shardCollectSource;
        }
        if (collectPhase instanceof FileUriCollectPhase) {
            return fileCollectSource;
        }
        if (collectPhase.maxRowGranularity() == RowGranularity.DOC && collectPhase.isPartitioned()) {
            // partitioned table without any shards; nothing to collect
            return VOID_COLLECT_SERVICE;
        }

        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        if (indexShards == null) {
            indexShards = locations.get(TableInfo.NULL_NODE_ID);
        }
        if (indexShards.size() == 0) {
            // select * from sys.nodes
            return singleRowSource;
        }

        assert indexShards.size() == 1 : "routing without shards that operate on non user-tables may only contain 1 index/table";
        CollectSource collectSource = nodeDocCollectSources.get(Iterables.getOnlyElement(indexShards.keySet()));
        if (collectSource == null) {
            throw new IllegalStateException("Can't resolve CollectService for collectPhase: " + collectPhase);
        }
        return collectSource;
    }

    static class VoidCollectSource implements CollectSource {

        @Override
        public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowDownstream downstream, JobCollectContext jobCollectContext) {
            return ImmutableList.of();
        }
    }
}
