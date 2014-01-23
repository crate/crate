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

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Task;
import io.crate.metadata.Functions;
import io.crate.operator.Input;
import io.crate.operator.InputCollectExpression;
import io.crate.operator.aggregation.AggregationCollector;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.planner.plan.AggregationNode;
import io.crate.planner.symbol.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class LocalAggregationTask implements Task<Object[][]> {

    protected final AggregationNode planNode;
    protected final List<Aggregation> aggregations;
    protected final List<AggregationCollector> collectors;

    protected final CollectExpression[] inputs;
    protected final Functions functions;

    protected List<ListenableFuture<Object[][]>> upstreamResults;

    static class VisitorContext {

        private Map<Integer, InputCollectExpression> inputExpressions = new HashMap<>();

        Input[] inputArray;
        int idx;

        private InputCollectExpression allocateInputExpression(int pos) {
            InputCollectExpression i = inputExpressions.get(pos);
            if (i != null) {
                return i;
            }
            i = new InputCollectExpression(pos);
            inputExpressions.put(pos, i);
            return i;
        }

        public InputCollectExpression[] getInputExpressions(){
            InputCollectExpression[] res = new InputCollectExpression[inputExpressions.size()];
            for (int i = 0; i < res.length; i++) {
                InputCollectExpression e = inputExpressions.get(i);
                Preconditions.checkNotNull(e, "Input column missing ", i);
                res[i] = e;
            }
            return res;
        }

    }

    class Visitor extends SymbolVisitor<VisitorContext, Symbol> {

        public VisitorContext process(AggregationNode node) {
            VisitorContext context = new VisitorContext();
            for (Symbol symbol : node.outputs()) {
                process(symbol, context);
            }
            return context;
        }

        @Override
        public Symbol visitAggregation(Aggregation aggregation, VisitorContext context) {
            assert (aggregation.fromStep() == Aggregation.Step.ITER);
            assert (aggregation.toStep() == Aggregation.Step.FINAL);
            aggregations.add(aggregation);
            AggregationFunction impl = (AggregationFunction) functions.get(aggregation.functionIdent());
            Preconditions.checkNotNull(impl, "AggregationFunction implementation not found", aggregation.functionIdent());

            context.inputArray = new Input[aggregation.inputs().size()];
            context.idx = 0;
            for (Symbol input : aggregation.inputs()) {
                process(input, context);
                context.idx++;
            }
            collectors.add(new AggregationCollector(aggregation, impl, context.inputArray));
            return aggregation;
        }


        @Override
        public Symbol visitInputColumn(InputColumn inputColumn, VisitorContext context) {
            context.inputArray[context.idx] = context.allocateInputExpression(inputColumn.index());
            return inputColumn;
        }


    }


    public LocalAggregationTask(AggregationNode node, Functions functions) {
        this.planNode = node;
        this.functions = functions;
        aggregations = new ArrayList<>();
        collectors = new ArrayList<>();

        Visitor v = new Visitor();
        VisitorContext vc = v.process(node);
        inputs = vc.getInputExpressions();

    }

    protected void startCollect() {
        for (CollectExpression i : inputs) {
            i.startCollect();
        }
        for (AggregationCollector ac : collectors) {
            ac.startCollect();
        }
    }

    protected void processUpstreamResult(Object[][] rows) {
        for (Object[] row : rows) {
            for (CollectExpression i : inputs) {
                i.setNextRow(row);
            }
            for (AggregationCollector ac : collectors) {
                ac.processRow();
            }
        }
    }

    protected Object[][] finishCollect() {
        Object[] row = new Object[collectors.size()];
        int idx = 0;
        for (AggregationCollector ac : collectors) {
            row[idx] = ac.finishCollect();
        }
        return new Object[][]{row};
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        upstreamResults = result;
    }

}
