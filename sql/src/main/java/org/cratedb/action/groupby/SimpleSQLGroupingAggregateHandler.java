package org.cratedb.action.groupby;

import org.cratedb.action.FieldLookup;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.io.IOException;
import java.util.List;

/**
 * this simple handler does not check whether we need to to a field lookup or not
 *
 * Use this class if you have to iterate through all the docs nonetheless
 */
public class SimpleSQLGroupingAggregateHandler implements SQLGroupingAggregateHandler {

    /**
     * handle Aggregates and never mind if you could stop or have to continue
     * collecting for a special AggState
     *
     * @param row the GroupByRow to apply the looked up value to
     * @param fieldLookup class that lets us make the lookup for a columnValue
     * @param aggregateExpressions the aggregate expressions found in the SQL-statement
     * @param aggFunctions the aggFunctions used in the current SQL-statement in correct order
     * @throws IOException
     */
    @Override
    public void handleAggregates(GroupByRow row, FieldLookup fieldLookup, List<AggExpr> aggregateExpressions, AggFunction[] aggFunctions) throws IOException {
        for (int i = 0; i < aggFunctions.length; i++) {
            AggExpr aggExpr = aggregateExpressions.get(i);
            AggFunction function = aggFunctions[i];
            Object value = null;

            if (aggExpr.parameterInfo != null) {
                value = fieldLookup.lookupField(aggExpr.parameterInfo.columnName);
            }
            function.iterate(row.aggStates.get(i), value);

        }
    }
}
