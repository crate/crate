package org.cratedb.action.groupby.aggregate;

import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.sql.SQLParseException;

public class AggExprFactory {

    public static AggExpr createAggExpr(String aggregateName, String aggregateParam) {
        ParameterInfo param;
        switch (aggregateName) {
            case "COUNT":
            case "COUNT(*)":
                param = new ParameterInfo();
                param.isAllColumn = true;
                return new AggExpr(CountAggFunction.NAME, param);
            case "MIN":
                param = new ParameterInfo();
                param.columnName = aggregateParam;
                return new AggExpr(MinAggFunction.NAME, param);
            default:
                throw new SQLParseException("Unsupported Aggregate function " + aggregateName);
        }
    }
}
