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
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.projectors.FlatProjectorChain;
import io.crate.operation.projectors.ProjectionToProjectorVisitor;
import io.crate.operation.projectors.Projector;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.QueryAndFetchNode;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;

import java.util.List;
import java.util.Set;

public class HandlerSideDataCollectOperation implements CollectOperation<Object[][]> {

    private static class NoOpCollectService implements CollectService {

        private static class NoOpCollector implements CrateCollector {

            private Projector downstream;

            @Override
            public void doCollect() throws Exception {
                // do nothing
                downstream.upstreamFinished();
            }

            @Override
            public void downstream(Projector downstream) {
                downstream.registerUpstream(this);
                this.downstream = downstream;
            }

            @Override
            public Projector downstream() {
                return downstream;
            }
        }

        @Override
        public CrateCollector getCollector(QueryAndFetchNode node, Projector projector) {
            NoOpCollector collector = new NoOpCollector();
            collector.downstream(projector);
            return collector;
        }
    }

    private static final NoOpCollectService NO_OP_COLLECT_SERVICE = new NoOpCollectService();

    private ESLogger logger = Loggers.getLogger(getClass());

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

    @Override
    public ListenableFuture<Object[][]> collect(QueryAndFetchNode queryAndFetchNode) {
        if (queryAndFetchNode.isPartitioned()) {
            // edge case: partitioned table without actual indices
            // no results
            return handleWithService(NO_OP_COLLECT_SERVICE, queryAndFetchNode);
        }
        if (queryAndFetchNode.maxRowGranularity() == RowGranularity.DOC) {
            // we assume information schema here
            return handleWithService(informationSchemaCollectService, queryAndFetchNode);
        } else if (queryAndFetchNode.maxRowGranularity() == RowGranularity.CLUSTER) {
            return handleCluster(queryAndFetchNode);
        } else if (queryAndFetchNode.maxRowGranularity() == RowGranularity.SHARD) {
            return handleWithService(unassignedShardsCollectService, queryAndFetchNode);
        } else {
            return Futures.immediateFailedFuture(new IllegalStateException("unsupported routing"));
        }
    }

    private ListenableFuture<Object[][]> handleCluster(QueryAndFetchNode queryAndFetchNode) {
        queryAndFetchNode = queryAndFetchNode.normalize(clusterNormalizer);
        if (queryAndFetchNode.whereClause().noMatch()) {
            return Futures.immediateFuture(TaskResult.EMPTY_RESULT.rows());
        }
        assert queryAndFetchNode.toCollect().size() > 0;

        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = implementationVisitor.process(queryAndFetchNode);
        List<Input<?>> inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();

        FlatProjectorChain projectorChain = new FlatProjectorChain(queryAndFetchNode.collectorProjections(), projectorVisitor);
        SimpleOneRowCollector collector = new SimpleOneRowCollector(
                inputs, collectExpressions, projectorChain.firstProjector());

        projectorChain.startProjections();
        try {
            collector.doCollect();
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
        return projectorChain.result();
    }

    private ListenableFuture<Object[][]> handleWithService(CollectService collectService, QueryAndFetchNode node) {
        FlatProjectorChain projectorChain = new FlatProjectorChain(node.collectorProjections(), projectorVisitor);
        CrateCollector collector = collectService.getCollector(node, projectorChain.firstProjector());
        projectorChain.startProjections();
        try {
            collector.doCollect();
        } catch (CollectionTerminatedException ex) {
            // ignore
        } catch (Exception e) {
            return Futures.immediateFailedFuture(e);
        }
        return projectorChain.result();
    }
}
