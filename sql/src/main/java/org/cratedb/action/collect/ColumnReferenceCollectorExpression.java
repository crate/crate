package org.cratedb.action.collect;

import org.cratedb.DataType;

public abstract class ColumnReferenceCollectorExpression<ReturnType> extends
        CollectorExpression<ReturnType> implements ColumnReferenceExpression {

    protected final String columnName;

    public ColumnReferenceCollectorExpression(String columnName) {
        this.columnName = columnName;
    }

    public String columnName() {
        return columnName;
    }

    @Override
    public String toString() {
        return columnName();
    }
}
