package io.crate.operator.aggregation;

import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.planner.symbol.Aggregation;

import java.util.List;

public class AggregationCollector implements RowCollector {

    private final Input[] inputs;
    private final Aggregation aggregation;
    private AggregationState aggregationState;
    private AggregationFunction aggregationFunction;

    public AggregationCollector(Aggregation a, AggregationFunction aggregationFunction, Input... inputs) {
        // TODO: implement othe start end steps
        assert (a.fromStep() == Aggregation.Step.ITER);
        assert (a.toStep() == Aggregation.Step.FINAL);
        this.inputs = inputs;
        this.aggregationFunction = aggregationFunction;
        this.aggregation = a;
    }

    public boolean startCollect() {
        aggregationState = aggregationFunction.newState();
        return true;
    }

    public boolean processRow() {
        return aggregationFunction.iterate(aggregationState, inputs);
    }


    public Object finishCollect() {
        aggregationState.terminatePartial();
        return aggregationState.value();
    }
}
