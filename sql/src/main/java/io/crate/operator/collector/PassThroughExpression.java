package io.crate.operator.collector;

import io.crate.operator.aggregation.CollectExpression;

/**
 * A collector expression which simply returns the whole row
 */
public class PassThroughExpression extends CollectExpression {

    private Object value;

    @Override
    public boolean setNextRow(Object... args) {
        this.value = args;
        return true;
    }

    @Override
    public Object value() {
        return value;
    }
}
