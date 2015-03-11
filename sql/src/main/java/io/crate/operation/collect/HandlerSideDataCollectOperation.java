/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.collect;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.ResultProvider;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Set;

public class HandlerSideDataCollectOperation implements CollectOperation, RowUpstream {

    private final EvaluatingNormalizer clusterNormalizer;
    private final ImplementationSymbolVisitor implementationVisitor;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final InformationSchemaCollectService informationSchemaCollectService;
    private final UnassignedShardsCollectService unassignedShardsCollectService;

    @Inject
    public HandlerSideDataCollectOperation(ClusterService clusterService,
                                           Settings settings,
                                           TransportActionProvider transportActionProvider,
                                           Functions functions,
                                           ReferenceResolver referenceResolver,
                                           InformationSchemaCollectService informationSchemaCollectService,
                                           UnassignedShardsCollectService unassignedShardsCollectService) {
        this.informationSchemaCollectService = informationSchemaCollectService;
        this.unassignedShardsCollectService = unassignedShardsCollectService;
        this.clusterNormalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
        this.implementationVisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);
        this.projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService, settings,
                transportActionProvider,
                implementationVisitor, clusterNormalizer);
    }

    /**
     * Collect the local result. This method expects the downstream to already have the projector chain inside in order
     * to prevent copying results by reusing the last projector as resultProvider.
     */
    @Override
    public void collect(CollectNode collectNode, RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        // NOTE: this method already expects the projector chain to be in the downsream
        if (collectNode.isPartitioned()) {
            // edge case: partitioned table without actual indices
            // no results
            downstream.registerUpstream(this).finish();
            return;
        }
        if (collectNode.maxRowGranularity() == RowGranularity.DOC) {
            // we assume information schema here
            handleWithService(informationSchemaCollectService, collectNode, downstream, ramAccountingContext);
        } else if (collectNode.maxRowGranularity() == RowGranularity.CLUSTER) {
            handleCluster(collectNode, downstream, ramAccountingContext);
        } else if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            handleWithService(unassignedShardsCollectService, collectNode, downstream, ramAccountingContext);
        } else {
            downstream.registerUpstream(this).fail(new IllegalStateException("unsupported routing"));
        }
    }

    private void handleCluster(CollectNode collectNode, RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        collectNode = collectNode.normalize(clusterNormalizer);
        if (collectNode.whereClause().noMatch()) {
            downstream.registerUpstream(this).finish();
        }
        assert collectNode.toCollect().size() > 0;

        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = implementationVisitor.process(collectNode);
        List<Input<?>> inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();
        SimpleOneRowCollector collector = new SimpleOneRowCollector(
                inputs, collectExpressions, downstream);
        collector.doCollect(ramAccountingContext);
    }

    private void handleWithService(CollectService collectService,
                                   CollectNode node,
                                   RowDownstream downstream, RamAccountingContext ramAccountingContext) {
        CrateCollector collector = collectService.getCollector(node, downstream);
        collector.doCollect(ramAccountingContext);
    }

    public ListenableFuture<Bucket> collect(CollectNode node, RamAccountingContext ramAccountingContext) {
        ResultProvider resultProvider;
        RowDownstream downstream;
        if (node.projections().isEmpty()) {
            downstream = resultProvider = new CollectingProjector();
        } else {
            FlatProjectorChain projectorChain = new FlatProjectorChain(node.projections(),
                    projectorVisitor, ramAccountingContext);
            downstream = projectorChain.firstProjector();
            if (!(projectorChain.lastProjector() instanceof ResultProvider)){
                projectorChain.append(new CollectingProjector());
            }
            resultProvider = (ResultProvider) projectorChain.lastProjector();
            projectorChain.startProjections();
        }
        collect(node, downstream, ramAccountingContext);
        return resultProvider.result();
    }
}
