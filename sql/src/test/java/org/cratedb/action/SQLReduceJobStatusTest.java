package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.cratedb.action.sql.ParsedStatement;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class SQLReduceJobStatusTest {

    @Test
    public void testToSortedArray() throws Exception {
        /**
         * after the reducing phase the GroupByRows are sorted and a limit is applied
         * test here that the sorting / limiting works correctly:
         */

        // TODO:
        ParsedStatement stmt = new ParsedStatement("" +
            "select count(*) from dummy group by dummyCol order by count(*)");

        stmt.orderByIndices = new ArrayList<OrderByColumnIdx>() {{
            add(new OrderByColumnIdx(0, false));
        }};
        stmt.idxMap = new Integer[] { 1 };
        stmt.limit(2);

        // result ist from 1 shard, limit is 2; order by first column
        SQLReduceJobStatus status = new SQLReduceJobStatus(stmt, 1,
            new HashMap<String, AggFunction>()
        );
        GroupByRow[] rows = new GroupByRow[]{
            new GroupByRow(new GroupByKey(new Object[]{ 1}), new CountAggState() {{ value = 3; }}),
            new GroupByRow(new GroupByKey(new Object[]{ 2}), new CountAggState() {{ value = 2; }}),
            new GroupByRow(new GroupByKey(new Object[]{ 3}), new CountAggState() {{ value = 45; }}),
            new GroupByRow(new GroupByKey(new Object[]{ 4}), new CountAggState() {{ value = 8; }}),
            new GroupByRow(new GroupByKey(new Object[]{ 5}), new CountAggState() {{ value = 40; }}),
        };

        SQLGroupByResult result = new SQLGroupByResult(Arrays.asList(rows));
        List<GroupByRow> sortedRows = new ArrayList<>(status.sortGroupByResult(result));

        assertEquals(2, sortedRows.size());
        assertEquals(45L, sortedRows.get(0).get(1));
        assertEquals(40L, sortedRows.get(1).get(1));
    }
}
