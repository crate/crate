package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SQLReduceJobStatusTest {

    @Test
    public void testToSortedArray() throws Exception {
        /**
         * after the reducing phase the GroupByRows are sorted and a limit is applied
         * test here that the sorting / limiting works correctly:
         */


        OrderByColumnIdx[] orderBy = new OrderByColumnIdx[] {
            new OrderByColumnIdx(0, false)
        };

        // first column maps to the CountAggState
        Integer[] idxMap = new Integer[] { 1 };

        // result ist from 1 shard, limit is 2; order by first column
        SQLReduceJobStatus status = new SQLReduceJobStatus(1, 2, orderBy);
        GroupByRow[] rows = new GroupByRow[]{
            new GroupByRow(idxMap, new GroupByKey(new Object[]{ 1}), new CountAggState() {{ value = 3; }}),
            new GroupByRow(idxMap, new GroupByKey(new Object[]{ 1}), new CountAggState() {{ value = 2; }}),
            new GroupByRow(idxMap, new GroupByKey(new Object[]{ 1}), new CountAggState() {{ value = 45; }}),
            new GroupByRow(idxMap, new GroupByKey(new Object[]{ 1}), new CountAggState() {{ value = 8; }}),
            new GroupByRow(idxMap, new GroupByKey(new Object[]{ 1}), new CountAggState() {{ value = 40; }}),
        };

        SQLGroupByResult result = new SQLGroupByResult(Arrays.asList(rows));
        GroupByRow[] sortedRows = status.toSortedArray(result);

        assertEquals(2, sortedRows.length);
        assertEquals(45L, sortedRows[0].get(0));
        assertEquals(40L, sortedRows[1].get(0));
    }
}
