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

import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.planner.plan.CollectNode;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.Set;

/**
 * collect local data from node/shards/docs on shards
 */
public class LocalDataCollectOperation {

    private ESLogger logger = Loggers.getLogger(getClass());

    private final ImplementationSymbolVisitor implementationSymbolVisitor;

    @Inject
    public LocalDataCollectOperation(ImplementationSymbolVisitor implementationSymbolVisitor) {
        this.implementationSymbolVisitor = implementationSymbolVisitor;
    }

    public Object[][] collect(String nodeId, CollectNode collectNode) {
        assert collectNode.routing().hasLocations();

        // only support node routing
        Preconditions.checkState(
                collectNode.routing().nodes().contains(nodeId) &&
                collectNode.routing().locations().get(nodeId) != null &&
                collectNode.routing().locations().get(nodeId).size() == 0,
                "unsupported routing"
        );

        // resolve Implementations
        ImplementationSymbolVisitor.Context ctx = implementationSymbolVisitor.process(collectNode);
        Input<?>[] inputs = ctx.topLevelInputs();
        Set<CollectExpression<?>> collectExpressions = ctx.collectExpressions();

        RowCollector<Object[][]> innerRowCollector = new SimpleOneRowCollector(inputs,  collectExpressions);
        if (innerRowCollector.startCollect()) {
            boolean carryOnProcessing;
            do {
                carryOnProcessing = innerRowCollector.processRow();
            } while(carryOnProcessing);
        }
        return innerRowCollector.finishCollect();
    }
}
