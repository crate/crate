package org.cratedb.action.groupby;

import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;

import java.io.IOException;
import java.util.List;

/**
 * Handles aggregate functions and their state while iterating over a shard-index
 *
 * This class takes care of applying new values to an existing GroupByRow.
 *
 * It has been extracted from SQLGroupingVisitor to allow different implementations.
 */
public interface SQLGroupingAggregateHandler {

    /**
     * handle aggregates while grouping
     *
     * @param row the GroupByRow to apply the looked up value to
     * @param aggregateExpressions the aggregate expressions found in the SQL-statement
     * @param aggFunctions the aggFunctions used in the current SQL-statement in correct order
     * @throws IOException
     */
    public void handleAggregates(
            GroupByRow row,
            List<AggExpr> aggregateExpressions, AggFunction[] aggFunctions) throws IOException;
}
