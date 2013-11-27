package org.cratedb.action.groupby;

import org.cratedb.action.GroupByFieldLookup;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.core.collections.CyclicIterator;

import java.util.Arrays;
import java.util.Map;

/**
 * GroupingCollector that is used for global aggregation without group by
 */
public class GlobalSQLGroupingCollector extends SQLGroupingCollector {
    private static final GroupByKey GLOBAL_AGGREGATE_GROUP_KEY = new GroupByKey(new Object[]{ 1 });
    private final CyclicIterator<String> reducerIter;

    public GlobalSQLGroupingCollector(ParsedStatement parsedStatement, GroupByFieldLookup groupByFieldLookup, Map<String, AggFunction> aggFunctionMap, String[] reducers) {
        super(parsedStatement, groupByFieldLookup, aggFunctionMap, reducers);

        assert parsedStatement.isGlobalAggregate();
        boolean hasDistinctAggExpr = false;
        for (AggExpr aggExpr : parsedStatement.aggregateExpressions) {
            if (aggExpr.isDistinct) {
                hasDistinctAggExpr = true;
                break;
            }
        }

        /**
         * distinct requires that one reducer has a complete set of all seenValues
         * in order for the {@link org.cratedb.action.groupby.aggregate.AggState#terminatePartial()}
         * to generate the correct value.
         */
        if (hasDistinctAggExpr) {
            reducerIter = new CyclicIterator<>(Arrays.asList(this.reducers[0]));
        } else {
            reducerIter = new CyclicIterator<>(Arrays.asList(this.reducers));
        }
    }

    /**
     * it's always the same
     * @return
     */
    @Override
    protected GroupByKey getGroupByKey() {
        return GLOBAL_AGGREGATE_GROUP_KEY;
    }

    /**
     * simply return the next reducer
     * @return
     */
    @Override
    protected String partitionByKey(String[] reducers, GroupByKey key) {
        return reducerIter.next();
    }
}
