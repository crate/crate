package org.cratedb.action;

import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class SQLReduceJobStatusTest {
    @Test
    public void testToSortedArray() throws Exception {

        OrderByColumnIdx[] orderBy = new OrderByColumnIdx[] {
            new OrderByColumnIdx(0, false)
        };
        SQLReduceJobStatus status = new SQLReduceJobStatus(1, 2, orderBy);
        Map<Integer, GroupByRow> resultAsMap = new HashMap<>();
        addRow(resultAsMap, 1, 3);
        addRow(resultAsMap, 2, 2);
        addRow(resultAsMap, 3, 45);
        addRow(resultAsMap, 4, 8);
        addRow(resultAsMap, 5, 40);

        // TODO:
        // SQLGroupByResult result = new SQLGroupByResult(resultAsMap);
        // GroupByRow[] sortedRows = status.toSortedArray(result);

        // assertEquals(2, sortedRows.length);
        // assertEquals(45L, ((AggState)sortedRows[0].get(0)).value());
        // assertEquals(40L, ((AggState)sortedRows[1].get(0)).value());
    }

    private void addRow(Map<Integer, GroupByRow> resultAsMap, int idx, final int aggValue) {
        GroupByRow row = new GroupByRow();
        // TODO:
        // row.aggStates.put(0, new CountAggState() {{ value = aggValue; }});
        resultAsMap.put(idx, row);
    }
}
