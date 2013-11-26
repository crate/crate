package org.cratedb.action.groupby.aggregate;

import org.cratedb.DataType;
import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.groupby.aggregate.max.MaxAggFunction;
import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.sql.SQLParseException;

public class AggExprFactory {

    public static AggExpr createAggExpr(String aggregateName, String aggregateParam, DataType dataType) {
        ParameterInfo param;
        switch (aggregateName) {
            case "COUNT":
                // TODO: count all not null values of a column
            case "COUNT(*)":
                param = new ParameterInfo();
                param.isAllColumn = true;
                return new AggExpr(CountAggFunction.NAME, param);
            case "MIN":
                param = new ParameterInfo();
                param.columnName = aggregateParam;
                param.dataType = dataType;
                return new AggExpr(MinAggFunction.NAME, param);
            case "MAX":
                param = new ParameterInfo();
                param.columnName = aggregateParam;
                param.dataType = dataType;
                return new AggExpr(MaxAggFunction.NAME, param);
            default:
                throw new SQLParseException("Unsupported Aggregate function " + aggregateName);
        }
    }
}
