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
}
