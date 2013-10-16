package org.cratedb.action.groupby.aggregate;

import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.parser.ColumnDescription;

public class AggExpr extends ColumnDescription {

    public String functionName;
    public ParameterInfo parameterInfo;

    public AggExpr(String functionName, ParameterInfo parameterInfo) {
        super(Types.AGGREGATE_COLUMN);
        this.functionName = functionName;
        this.parameterInfo = parameterInfo;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AggExpr)) return false;

        AggExpr aggExpr = (AggExpr) o;

        if (!functionName.equals(aggExpr.functionName)) return false;
        if (!parameterInfo.equals(aggExpr.parameterInfo)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = functionName.hashCode();
        result = 31 * result + parameterInfo.hashCode();
        return result;
    }
}
