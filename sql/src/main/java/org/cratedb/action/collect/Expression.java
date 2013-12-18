package org.cratedb.action.collect;

import org.cratedb.DataType;

public interface Expression<ReturnType> {

    public ReturnType evaluate();

    public DataType returnType();

}
