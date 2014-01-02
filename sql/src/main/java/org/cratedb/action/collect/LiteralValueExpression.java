package org.cratedb.action.collect;

import org.cratedb.DataType;

public class LiteralValueExpression extends CollectorExpression {

    private final Object value;

    public LiteralValueExpression(Object value) {
        this.value = value;
    }

    @Override
    public Object evaluate() {
        return value;
    }

    @Override
    public DataType returnType() {
        return DataType.OBJECT;
    }
}
