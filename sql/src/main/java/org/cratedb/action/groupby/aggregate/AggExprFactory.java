package org.cratedb.action.groupby.aggregate;

import org.cratedb.DataType;
import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.avg.AvgAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.groupby.aggregate.max.MaxAggFunction;
import org.cratedb.action.groupby.aggregate.min.MinAggFunction;
import org.cratedb.action.groupby.aggregate.sum.SumAggFunction;
import org.cratedb.sql.SQLParseException;

public class AggExprFactory {

    public static AggExpr createAggExpr(String aggregateName, String aggregateParam, DataType dataType) {
        switch (aggregateName) {
            case "COUNT":
                // TODO: count all not null values of a column
            case "COUNT(*)":
                return new AggExpr(CountAggFunction.NAME, ParameterInfo.allColumnParameterInfo());
            case "MIN":
                return new AggExpr(MinAggFunction.NAME,
                        ParameterInfo.columnParameterInfo(aggregateParam, dataType));
            case "MAX":
                return new AggExpr(MaxAggFunction.NAME,
                        ParameterInfo.columnParameterInfo(aggregateParam, dataType));
            case "SUM":
                return new AggExpr(SumAggFunction.NAME,
                        ParameterInfo.columnParameterInfo(aggregateParam, dataType));
            case "AVG":
                return new AggExpr(AvgAggFunction.NAME,
                        ParameterInfo.columnParameterInfo(aggregateParam, dataType));
            default:
                throw new SQLParseException("Unsupported Aggregate function " + aggregateName);
        }
    }
}
