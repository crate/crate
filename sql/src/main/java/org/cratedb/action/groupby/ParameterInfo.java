package org.cratedb.action.groupby;

import org.cratedb.DataType;

/**
 * used to specify the parameters of the aggregateExpressions.
 *
 *  e.g.  avg(age) -> columnName: age; dataType: Integer
 *
 * this should in the future be replaced with some kind of expression class that implements
 * a evaluate() method.
 */
public class ParameterInfo {

    public String columnName = null;
    public DataType dataType = null; // dataType of the column to aggregate on

    public ParameterInfo(String columnName, DataType dataType) {
        this.columnName = columnName;
        this.dataType = dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ParameterInfo that = (ParameterInfo) o;

        if (columnName != null ? !columnName.equals(that.columnName) : that.columnName != null)
            return false;
        if (dataType != that.dataType) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = columnName != null ? columnName.hashCode() : 0;
        result = 31 * result + (dataType != null ? dataType.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return columnName;
    }
}
