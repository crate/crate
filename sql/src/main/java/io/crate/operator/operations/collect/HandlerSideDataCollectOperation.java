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

package io.crate.operator.operations.collect;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.collector.CrateCollector;
import io.crate.operator.collector.SimpleOneRowCollector;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.projectors.NoopProjector;
import io.crate.operator.projectors.ProjectionToProjectorVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import org.apache.lucene.search.CollectionTerminatedException;
import org.cratedb.Constants;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.*;

public class HandlerSideDataCollectOperation implements CollectOperation<Object[][]> {

    private ESLogger logger = Loggers.getLogger(getClass());

    private final EvaluatingNormalizer clusterNormalizer;
    private final ImplementationSymbolVisitor implementationVisitor;
    private final ProjectionToProjectorVisitor projectorVisitor;
    private final InformationSchemaCollectService informationSchemaCollectService;
    private final UnassignedShardsCollectService unassignedShardsCollectService;

    @Inject
    public HandlerSideDataCollectOperation(Functions functions,
                                           ReferenceResolver referenceResolver,
                                           InformationSchemaCollectService informationSchemaCollectService,
                                           UnassignedShardsCollectService unassignedShardsCollectService) {
        this.informationSchemaCollectService = informationSchemaCollectService;
        this.unassignedShardsCollectService = unassignedShardsCollectService;
        this.clusterNormalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
        this.implementationVisitor = new ImplementationSymbolVisitor(referenceResolver, functions, RowGranularity.CLUSTER);
        this.projectorVisitor = new ProjectionToProjectorVisitor(implementationVisitor);
    }

    @Override
    public ListenableFuture<Object[][]> collect(CollectNode collectNode) {
        if (collectNode.isRouted()) {
            // unassigned shards
            assert collectNode.routing().locations().containsKey(null)
                && collectNode.maxRowGranularity() == RowGranularity.SHARD;
        }

        SettableFuture<Object[][]> result = SettableFuture.create();
        if (collectNode.maxRowGranularity() == RowGranularity.DOC) {
            // we assume information schema here
            try {
                result.set(handleWithService(informationSchemaCollectService, collectNode));
            } catch (Exception e) {
                result.setException(e);
            }
        } else if (collectNode.maxRowGranularity() == RowGranularity.CLUSTER) {
            result.set(handleCluster(collectNode));
        } else if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            try {
                result.set(handleWithService(unassignedShardsCollectService, collectNode));
            } catch (Exception e) {
                result.setException(e);
            }
        } else {
            throw new CrateException("unsupported routing");
        }
        return result;
    }

    protected List<Projector> extractProjectors(CollectNode collectNode) {
        List<Projector> projectors = new ArrayList<>(collectNode.projections().size());
        if (collectNode.projections().size() == 0) {
            projectors.add(new NoopProjector());
        } else {
            projectors = projectorVisitor.process(collectNode.projections());
        }
        assert projectors.size() >= 1 : "no projectors";
        return projectors;
    }

    private Object[][] handleCluster(CollectNode collectNode) {
        collectNode = collectNode.normalize(clusterNormalizer);
        if (collectNode.whereClause().noMatch()) {
            return Constants.EMPTY_RESULT;
        }
        assert collectNode.toCollect().size() > 0;

        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = implementationVisitor.process(collectNode);
        List<Input<?>> inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();

        List<Projector> projectors = extractProjectors(collectNode);

        new SimpleOneRowCollector(inputs, collectExpressions, projectors.get(0)).doCollect();
        projectors.get(0).finishProjection();

        Object[][] collected = projectors.get(projectors.size() - 1).getRows();
        if (logger.isTraceEnabled()) {
            logger.trace("collected {} on {}-level",
                    Objects.toString(Arrays.asList(collected[0])),
                    collectNode.maxRowGranularity().name().toLowerCase()
            );
        }
        return collected;
    }

    private Object[][] handleWithService(CollectService collectService, CollectNode node) throws Exception {
        List<Projector> projectors = extractProjectors(node);
        projectors.get(0).startProjection();
        CrateCollector collector = collectService.getCollector(node, projectors.get(0));
        try {
            collector.doCollect();
        } catch (CollectionTerminatedException ex) {
            // ignore
        }
        projectors.get(0).finishProjection();
        Object[][] collected = projectors.get(projectors.size() - 1).getRows();
        return collected;
    }
}
