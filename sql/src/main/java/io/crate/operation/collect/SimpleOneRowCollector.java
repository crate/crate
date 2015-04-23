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
import io.crate.core.collections.Row;
import io.crate.operation.Input;
import io.crate.operation.InputRow;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;

import java.util.List;
import java.util.Set;

/**
 * Simple Collector that only collects one row and does not support any query or aggregation
 */
public class SimpleOneRowCollector extends AbstractRowCollector<Row> implements CrateCollector {

    private final Set<CollectExpression<?>> collectExpressions;
    private final InputRow row;
    private RowDownstreamHandle downstream;

    public SimpleOneRowCollector(List<Input<?>> inputs,
                                 Set<CollectExpression<?>> collectExpressions,
                                 RowDownstream downstream) {
        this.row = new InputRow(inputs);
        this.collectExpressions = collectExpressions;
        this.downstream = downstream.registerUpstream(this);
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
        downstream.setNextRow(row);
        return false;
    }

    @Override
    public Row finishCollect() {
        downstream.finish();
        return row;
    }

    @Override
    public void doCollect(JobCollectContext jobCollectContext) {
        collect(jobCollectContext.ramAccountingContext());
        jobCollectContext.close();
    }
}
