package org.cratedb.action.groupby;

import org.cratedb.DataType;
import org.cratedb.core.StringUtils;

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

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParameterInfo)) return false;

        ParameterInfo that = (ParameterInfo) o;

        if (columnName != null && !columnName.equals(that.columnName)) return false;
        if (that.columnName != null && !that.columnName.equals(columnName)) return false;
        if (that.dataType != this.dataType) return false;

        return true;
    }

    public int hashCode() {
        return columnName.hashCode();
    }

    @Override
    public String toString() {
        return columnName;
    }

    public String toOutputString() {
        return columnName.contains(".") ? StringUtils.dottedToSqlPath(columnName) : columnName;
    }
}
