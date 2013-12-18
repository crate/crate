package org.cratedb.action.groupby;

import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.io.IOException;
import java.util.List;

/**
 * This handler does check if it can stop collecting for a certain AggState
 *
 * Use this class if {@link org.cratedb.action.sql.ParsedStatement}.hasStoppableAggregate is true
 */
public class CheckingSQLGroupingAggregateHandler implements SQLGroupingAggregateHandler {

    @Override
    public void handleAggregates(
            GroupByRow row,
            List<AggExpr> aggregateExpressions, AggFunction[] aggFunctions) throws IOException {
        for (int i = 0; i < aggFunctions.length; i++) {
            if (row.continueCollectingFlags[i]) {
                AggExpr aggExpr = aggregateExpressions.get(i);
                AggFunction function = aggFunctions[i];
                Object value;
                if (aggExpr.expression != null){
                    value = aggExpr.expression.evaluate();
                } else {
                    value = null;
                }
                row.continueCollectingFlags[i] = function.iterate(row.aggStates.get(i), value);
            }
        }
    }
}
