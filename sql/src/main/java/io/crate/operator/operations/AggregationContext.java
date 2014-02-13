package io.crate.operator.operations;

import io.crate.operator.Input;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.planner.symbol.Aggregation;

import java.util.ArrayList;
import java.util.List;

public class AggregationContext {
    private final AggregationFunction impl;
    private final Aggregation symbol;
    private final List<Input<?>> inputs = new ArrayList<>();

    public AggregationContext(AggregationFunction aggregationFunction, Aggregation aggregation) {
        this.impl = aggregationFunction;
        this.symbol = aggregation;
    }

    public void addInput(Input<?> input) {
        inputs.add(input);
    }

    public AggregationFunction function() {
        return impl;
    }

    public Aggregation symbol() {
        return symbol;
    }

    public Input<?>[] inputs () {
        return inputs.toArray(new Input[inputs.size()]);
    }
}
