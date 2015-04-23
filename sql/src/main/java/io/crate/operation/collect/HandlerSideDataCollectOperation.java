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

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Set;

public class HandlerSideDataCollectOperation implements CollectOperation {

    private final EvaluatingNormalizer clusterNormalizer;
    private final ImplementationSymbolVisitor implementationVisitor;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final InformationSchemaCollectService informationSchemaCollectService;
    private final UnassignedShardsCollectService unassignedShardsCollectService;

    @Inject
    public HandlerSideDataCollectOperation(ClusterService clusterService,
                                           Settings settings,
                                           TransportActionProvider transportActionProvider,
                                           BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                           Functions functions,
                                           ReferenceResolver referenceResolver,
                                           InformationSchemaCollectService informationSchemaCollectService,
                                           UnassignedShardsCollectService unassignedShardsCollectService) {
        this.informationSchemaCollectService = informationSchemaCollectService;
        this.unassignedShardsCollectService = unassignedShardsCollectService;
        this.clusterNormalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
        this.implementationVisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);
        this.projectorVisitor = new ProjectionToProjectorVisitor(
                clusterService,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                implementationVisitor,
                clusterNormalizer);
    }

    @Override
    public ListenableFuture<Bucket> collect(CollectNode collectNode, RamAccountingContext ramAccountingContext) {
        if (collectNode.isPartitioned()) {
            // edge case: partitioned table without actual indices
            // no results
            return Futures.immediateFuture(TaskResult.EMPTY_RESULT.rows());
        }
        if (collectNode.maxRowGranularity() == RowGranularity.DOC) {
            // we assume information schema here
            return handleWithService(informationSchemaCollectService, collectNode, ramAccountingContext);
        } else if (collectNode.maxRowGranularity() == RowGranularity.CLUSTER) {
            return handleCluster(collectNode, ramAccountingContext);
        } else if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            return handleWithService(unassignedShardsCollectService, collectNode, ramAccountingContext);
        } else {
            return Futures.immediateFailedFuture(new IllegalStateException("unsupported routing"));
        }
    }

    private ListenableFuture<Bucket> handleCluster(CollectNode collectNode, RamAccountingContext ramAccountingContext) {
        collectNode = collectNode.normalize(clusterNormalizer);
        if (collectNode.whereClause().noMatch()) {
            return Futures.immediateFuture(TaskResult.EMPTY_RESULT.rows());
        }
        assert collectNode.toCollect().size() > 0;

        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = implementationVisitor.process(collectNode);
        List<Input<?>> inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();

        FlatProjectorChain projectorChain = new FlatProjectorChain(
                collectNode.projections(), projectorVisitor, ramAccountingContext);
        SimpleOneRowCollector collector = new SimpleOneRowCollector(
                inputs, collectExpressions, projectorChain.firstProjector());
        projectorChain.startProjections();
        try {
            collector.doCollect(ramAccountingContext);
        } catch (CollectionAbortedException e) {
            // ignore
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
        return projectorChain.resultProvider().result();
    }

    private ListenableFuture<Bucket> handleWithService(CollectService collectService,
                                                           CollectNode node,
                                                           RamAccountingContext ramAccountingContext) {
        FlatProjectorChain projectorChain = new FlatProjectorChain(node.projections(),
                projectorVisitor, ramAccountingContext);
        CrateCollector collector = collectService.getCollector(node, projectorChain.firstProjector());
        projectorChain.startProjections();
        try {
            collector.doCollect(ramAccountingContext);
        } catch (CollectionAbortedException ex) {
            // ignore
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
        return projectorChain.resultProvider().result();
    }
}
