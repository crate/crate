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

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.Input;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import org.elasticsearch.common.inject.Inject;


import java.util.List;
import java.util.Set;

public class HandlerSideDataCollectOperation implements CollectOperation, RowUpstream {

    private final EvaluatingNormalizer clusterNormalizer;
    private final ImplementationSymbolVisitor implementationVisitor;
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
    }

    private void handleCluster(CollectNode collectNode, RowDownstream downstream, JobCollectContext jobCollectContext) {
        collectNode = collectNode.normalize(clusterNormalizer);
        if (collectNode.whereClause().noMatch()) {
            downstream.registerUpstream(this).finish();
            return;
        }
        assert collectNode.toCollect().size() > 0;

        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = implementationVisitor.process(collectNode);
        List<Input<?>> inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();
        SimpleOneRowCollector collector = new SimpleOneRowCollector(
                inputs, collectExpressions, downstream);
        collector.doCollect(jobCollectContext);
    }

    private void handleWithService(CollectService collectService,
                                   CollectNode node,
                                   RowDownstream rowDownstream,
                                   JobCollectContext jobCollectContext) {
        CrateCollector collector = collectService.getCollector(node, rowDownstream); // calls projector.registerUpstream()
        collector.doCollect(jobCollectContext);
    }

    @Override
    public void collect(CollectNode collectNode, RowDownstream downstream, JobCollectContext jobCollectContext) {
        if (collectNode.isPartitioned()) {
            // edge case: partitioned table without actual indices
            // no results
            downstream.registerUpstream(this).finish();
        } else if (collectNode.maxRowGranularity() == RowGranularity.DOC) {
            // we assume information schema here
            handleWithService(informationSchemaCollectService, collectNode, downstream, jobCollectContext);
        } else if (collectNode.maxRowGranularity() == RowGranularity.CLUSTER) {
            handleCluster(collectNode, downstream, jobCollectContext);
        } else if (collectNode.maxRowGranularity() == RowGranularity.SHARD) {
            handleWithService(unassignedShardsCollectService, collectNode, downstream, jobCollectContext);
        } else {
            downstream.registerUpstream(this).fail(new IllegalStateException("unsupported routing"));
        }
        jobCollectContext.close();
    }
}
