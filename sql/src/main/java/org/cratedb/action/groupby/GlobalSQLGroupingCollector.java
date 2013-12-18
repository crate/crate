package org.cratedb.action.groupby;

import org.cratedb.action.collect.CollectorContext;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.key.GlobalRows;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;

import java.util.Map;

/**
 * GroupingCollector that is used for global aggregation without group by
 */
public class GlobalSQLGroupingCollector extends SQLGroupingCollector {

    public GlobalSQLGroupingCollector(ParsedStatement parsedStatement,
                                      CollectorContext context,
                                      Map<String, AggFunction> aggFunctionMap,
                                      int numReducers) {
        super(parsedStatement, context, aggFunctionMap, numReducers);
        assert parsedStatement.isGlobalAggregate();
    }

    @Override
    protected Rows newRows() {
        return new GlobalRows(numReducers, parsedStatement);
    }
}
