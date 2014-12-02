/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.breaker.RamAccountingContext;
import io.crate.operation.Input;
import io.crate.operation.projectors.Projector;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Simple Collector that only collects one row and does not support any query or aggregation
 */
public class SimpleOneRowCollector extends AbstractRowCollector<Object[]> implements CrateCollector {

    private final List<Input<?>> inputs;
    private final Set<CollectExpression<?>> collectExpressions;
    private final Object[] result;
    private Projector downstream;

    public SimpleOneRowCollector(List<Input<?>> inputs,
                                 Set<CollectExpression<?>> collectExpressions,
                                 Projector downstream) {
        this.inputs = inputs;
        this.result = new Object[inputs.size()];
        this.collectExpressions = collectExpressions;
        downstream(downstream);
    }

    @Override
    public boolean startCollect(RamAccountingContext ramAccountingContext) {
        for (CollectExpression<?> collectExpression : collectExpressions) {
            collectExpression.startCollect();
        }
        return true;
    }

    @Override
    public boolean processRow() {
        int i = 0;
        if (inputs != null) {
            for (Input<?> input : inputs) {
                result[i++] = input.value();
            }
        }
        downstream.setNextRow(result);
        return false;
    }

    @Override
    public Object[] finishCollect() {
        downstream.upstreamFinished();
        return result;
    }

    @Override
    public void doCollect(RamAccountingContext ramAccountingContext) throws IOException {
        collect(ramAccountingContext);
    }

    @Override
    public void downstream(Projector downstream) {
        downstream.registerUpstream(this);
        this.downstream = downstream;
    }
}
