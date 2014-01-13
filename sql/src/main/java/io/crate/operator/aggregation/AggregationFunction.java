package io.crate.operator.aggregation;

import io.crate.metadata.FunctionImplementation;
import io.crate.operator.Input;

import java.util.List;

public abstract class AggregationFunction<T extends AggregationState> implements FunctionImplementation {


    /**
     * Apply the columnValue to the argument AggState using the logic in this AggFunction
     *
     * @param state the aggregation state for the iteration
     * @param args  the arguments according to FunctionInfo.argumentTypes
     * @return false if we do not need any further iteration for this state
     */
    public abstract boolean iterate(T state, Input... args);


    /**
     * Creates a new state for this aggregation
     *
     * @return a new state instance
     */
    public abstract T newState();

}
