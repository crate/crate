package org.cratedb.action.groupby.aggregate;

import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.sql.SQLParseException;

public class AggExprFactory {

    public static AggExpr createAggExpr(String aggregateName) {

        switch (aggregateName) {
            case "COUNT(*)":
                ParameterInfo param = new ParameterInfo();
                param.isAllColumn = true;
                return new AggExpr(CountAggFunction.NAME, param);

            default:
                throw new SQLParseException("Unsupported Aggregate function " + aggregateName);
        }
    }
}
