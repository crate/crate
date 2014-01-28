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
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.operator.operations.ImplementationSymbolVisitor;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;

public class SimpleShardCollector implements RowCollector<Object[][]>, Runnable {

    public static final Object[] EMPTY_ROW = new Object[0];
    private final Functions functions;
    private final ReferenceResolver referenceResolver;
    private final BlockingQueue<Object[]> resultQueue;
    private Input<?>[] inputs;
    private Object[] result;

    private boolean doCollect = true;

    public SimpleShardCollector(Functions functions,
                                ReferenceResolver referenceResolver,
                                Reference[] shardOrDocLevelReferences,
                                Optional<Function> whereClause,
                                BlockingQueue<Object[]> resultQueue) {
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.resultQueue = resultQueue;

        // normalize on shard level
        if (whereClause.isPresent()) {
            EvaluatingNormalizer normalizer = new EvaluatingNormalizer(this.functions, RowGranularity.SHARD, this.referenceResolver);
            Symbol normalizedWhereClause = normalizer.process(whereClause.get(), null);
            if (normalizedWhereClause.symbolType() == SymbolType.NULL_LITERAL ||
                    (normalizedWhereClause.symbolType()==SymbolType.BOOLEAN_LITERAL && !((BooleanLiteral) normalizedWhereClause).value())) {
                inputs = new Input<?>[0];
                result = EMPTY_ROW;
                doCollect = false;
            }
        }
        if (doCollect) {
            // get Inputs
            ImplementationSymbolVisitor visitor = new ImplementationSymbolVisitor(
                    this.referenceResolver,
                    this.functions,
                    RowGranularity.SHARD // TODO: doc level?
            );
            ImplementationSymbolVisitor.Context ctx = visitor.process(shardOrDocLevelReferences);
            // TODO: when also collecting on doc level, resolve/normalize shard level expression and store them
            // for easy access
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
        try {
            resultQueue.put(result);
        } catch(InterruptedException e) {
            // TODO: handle or ignore
        }
        return false;
    }

    @Override
    public Object[][] finishCollect() {
        try {
            resultQueue.put(EMPTY_ROW); // signal termination
        } catch(InterruptedException e) {
            // TODO: handle or ignore.
        }
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
