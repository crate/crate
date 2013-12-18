package org.cratedb.action.collect;

import org.cratedb.DataType;

public class LiteralExpression implements Expression<Object> {

    public static final LiteralExpression INSTANCE = new LiteralExpression();

    @Override
    public Object evaluate() {
        return null;
    }

    @Override
    public DataType returnType() {
        return null;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        return (obj instanceof LiteralExpression);
    }

}
