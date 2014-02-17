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

package io.crate.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Task;
import io.crate.operator.RowCollector;
import io.crate.operator.collector.PassThroughExpression;
import io.crate.operator.collector.SimpleRangeCollector;
import io.crate.operator.collector.SortingRangeCollector;
import io.crate.planner.plan.TopNNode;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LocalTopNTask implements Task<Object[][]> {

    protected final TopNNode planNode;

    private final PassThroughExpression input;
    protected AtomicBoolean done = new AtomicBoolean(false);

    protected List<ListenableFuture<Object[][]>> upstreamResults;
    private RowCollector<Object[][]> collector;

    public LocalTopNTask(TopNNode node) {
        this.planNode = node;
        // TODO: remove the PassThroughExpression here, since in the end we will need an input per column
        // anyways in order to evaluate aritmetics etc.
        this.input = new PassThroughExpression();

        if (node.isOrdered()) {
            collector = new SortingRangeCollector(node.offset(), node.limit(),
                    node.orderBy(), node.reverseFlags(), input);
        } else {
            collector = new SimpleRangeCollector(node.offset(), node.limit(), input);
        }
    }


    protected void startCollect() {
        input.startCollect();
        done.set(!collector.startCollect());
    }

    protected void processUpstreamResult(Object[][] rows) {
        boolean stop = done.get();
        if (stop) return;
        for (Object[] row : rows) {
            stop = !input.setNextRow(row);
            if (!stop) {
                stop = !collector.processRow();
            }
            if (stop) {
                done.set(stop);
                return;
            }
        }
    }

    protected Object[][] finishCollect() {
        return collector.finishCollect();
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        upstreamResults = result;
    }


}
