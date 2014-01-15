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
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import java.util.ArrayList;
import java.util.List;

public abstract class LocalAggregationTask implements Task<Object[][]> {

    protected final AggregationNode planNode;
    protected final List<Aggregation> aggregations;
    protected final List<AggregationCollector> collectors;

    protected final CollectExpression[] inputs;

    protected List<ListenableFuture<Object[][]>> upstreamResults;

    public LocalAggregationTask(AggregationNode node) {
        this.planNode = node;

        aggregations = new ArrayList<>();
        for (Symbol s : planNode.symbols()) {
            if (s.symbolType() == SymbolType.AGGREGATION) {
                aggregations.add((Aggregation) s);
            }
        }

        inputs = new CollectExpression[planNode.inputs().size()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = new InputCollectExpression(i);
        }

        collectors = new ArrayList<>(aggregations.size());

        for (Aggregation a : aggregations) {
            // TODO: implement othe start end steps
            assert (a.fromStep() == Aggregation.Step.ITER);
            assert (a.toStep() == Aggregation.Step.FINAL);

            AggregationFunction impl = (AggregationFunction) Functions.get(a.functionIdent());
            Preconditions.checkNotNull(impl, "AggregationFunction implementation not found", a.functionIdent());

            Input[] aggInputs = new Input[a.inputs().size()];
            for (int i = 0; i < aggInputs.length; i++) {
                aggInputs[i] = inputs[planNode.inputs().indexOf(a.inputs().get(i))];
            }
            // TODO: distinct input
            if (a.fromStep() == Aggregation.Step.ITER) {
                collectors.add(new AggregationCollector(a, impl, aggInputs));

            } else {
                throw new RuntimeException("Step not implemented " + a.fromStep());
            }

        }
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
                ac.nextRow();
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
