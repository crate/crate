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

import com.google.common.base.Optional;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.NormalizationHelper;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.AbstractRowCollector;
import io.crate.operator.Input;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.planner.RowGranularity;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Function;

import java.util.Arrays;

public class SimpleShardCollector extends AbstractRowCollector<Object[][]> implements Runnable {

    public static final Object[] EMPTY_ROW = new Object[0];
    private Input<?>[] inputs;
    private Object[] result;
    private final Projector downStreamProjector;
    private final LocalDataCollectOperation.ShardCollectFuture shardCollectFuture;

    private boolean doCollect = true;

    public SimpleShardCollector(Functions functions,
                                ReferenceResolver referenceResolver,
                                CollectNode collectNode,
                                Projector downStreamProjector,
                                LocalDataCollectOperation.ShardCollectFuture shardCollectFuture) {
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(functions, RowGranularity.SHARD, referenceResolver);
        this.downStreamProjector = downStreamProjector;
        this.shardCollectFuture = shardCollectFuture;

        // normalize on shard level
        Optional<Function> whereClause = collectNode.whereClause();
        if (whereClause.isPresent()) {

            if (whereClause.isPresent() && NormalizationHelper.evaluatesToFalse(whereClause.get(), normalizer)) {
                inputs = new Input<?>[0];
                result = EMPTY_ROW;
                doCollect = false;
            }

        }
        if (doCollect) {
            // get Inputs
            ImplementationSymbolVisitor visitor = new ImplementationSymbolVisitor(
                    referenceResolver,
                    functions,
                    RowGranularity.SHARD // TODO: doc level?
            );
            ImplementationSymbolVisitor.Context ctx = visitor.process(collectNode.toCollect());
            // assume we got only resolvable inputs without the need to collect anything
            inputs = ctx.topLevelInputs();
            result = new Object[inputs.length];
        }
    }

    @Override
    public boolean startCollect() {
        return doCollect;
    }

    /**
     * only does one simple run
     */
    @Override
    public boolean processRow() {
        Arrays.fill(result, null);
        for (int i=0, length=inputs.length; i<length; i++) {
            result[i] = inputs[i].value();
        }
        downStreamProjector.setNextRow(result);
        return false; // only one row will be processed on shard level, stop here
    }

    @Override
    public Object[][] finishCollect() {
        shardCollectFuture.shardFinished();
        return null;
    }

    @Override
    public void run() {
        if (startCollect()) {
            boolean carryOn;
            do {
                carryOn = processRow();
            } while(carryOn);
        }
        finishCollect();
    }
}
