package io.crate.operator;

import io.crate.operator.aggregation.CollectExpression;

public class InputCollectExpression<ReturnType> extends CollectExpression<ReturnType> {

    private final int position;
    private ReturnType value;

    public InputCollectExpression(int position) {
        this.position = position;
    }

    @Override
    public boolean setNextRow(Object... args) {
        value = (ReturnType) args[position];
        return true;
    }

    @Override
    public ReturnType value() {
        return value;
    }
}
