package io.crate.operation;

import io.crate.data.Input;
import io.crate.operation.aggregation.AggregationFunction;

import java.util.ArrayList;
import java.util.List;

public class AggregationContext {

    private final AggregationFunction impl;
    private final List<Input<?>> inputs = new ArrayList<>();

    public AggregationContext(AggregationFunction aggregationFunction) {
        this.impl = aggregationFunction;
    }

    public void addInput(Input<?> input) {
        inputs.add(input);
    }

    public AggregationFunction function() {
        return impl;
    }

    public Input<?>[] inputs() {
        return inputs.toArray(new Input[inputs.size()]);
    }
}
