package io.crate.operator.aggregation;

import io.crate.operator.Input;

public abstract class CollectExpression<ReturnType> implements Input<ReturnType> {

    /**
     * An expression which gets evaluated in the collect phase
     */
    //public abstract class CollectorExpression<ReturnType> implements Expression<ReturnType> {

    public void startCollect() {
    }

    public abstract boolean setNextRow(Object... args);

}

