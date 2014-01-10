package org.cratedb.action.collect;

import org.cratedb.DataType;

public class NullExpression implements Expression<Object> {

    public static final NullExpression INSTANCE = new NullExpression();

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
        return (obj instanceof NullExpression);
    }

}
