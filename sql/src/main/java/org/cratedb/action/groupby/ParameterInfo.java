package org.cratedb.action.groupby;

public class ParameterInfo {

    public boolean isAllColumn;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ParameterInfo)) return false;

        ParameterInfo that = (ParameterInfo) o;

        if (isAllColumn != that.isAllColumn) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return (isAllColumn ? 1 : 0);
    }
}
