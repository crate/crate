package org.cratedb.action.groupby;

import org.cratedb.DataType;

/**
 * used to specify the parameters of the aggregateExpressions.
 *
 *  e.g. count(*) ->  ParameterInfo with isAllColumn true
 *
 *  intended to be extended to be used for example for avg(columnName)
 */
public class ParameterInfo {

    public boolean isAllColumn;
    public String columnName = null;
    public DataType dataType = null; // dataType of the column to aggregate on

    /**
     * empty constructor, only used by
     */
    private ParameterInfo() {}

    public ParameterInfo(String columnName, DataType dataType, boolean isAllColumn) {
        this.columnName = columnName;
        this.dataType = dataType;
        this.isAllColumn = isAllColumn;
    }

    public static ParameterInfo columnParameterInfo(String columnName, DataType dataType) {
        return new ParameterInfo(columnName, dataType, false);
    }

    public static ParameterInfo allColumnParameterInfo() {
        return new ParameterInfo(null, null, true);
    }

    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParameterInfo)) return false;

        ParameterInfo that = (ParameterInfo) o;

        if (isAllColumn != that.isAllColumn) return false;

        if (columnName != null && !columnName.equals(that.columnName)) return false;

        if (that.columnName != null && !that.columnName.equals(columnName)) return false;

        if (that.dataType != this.dataType) return false;

        return true;
    }

    public int hashCode() {
        return (isAllColumn ? 1 : columnName.hashCode());
    }

    @Override
    public String toString() {
        return (isAllColumn ? "*" : columnName);
    }
}
