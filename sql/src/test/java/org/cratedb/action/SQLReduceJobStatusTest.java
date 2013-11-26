package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.ParameterInfo;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.parser.ColumnDescription;
import org.cratedb.action.sql.OrderByColumnIdx;
import org.cratedb.action.sql.ParsedStatement;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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

        stmt.resultColumnList = new ArrayList<ColumnDescription>() {{
            add(new AggExpr(
                CountAggFunction.COUNT_ROWS_NAME,
                new ParameterInfo() {{ isAllColumn = true; }}
            ));
        }};
        stmt.orderByIndices = new ArrayList<OrderByColumnIdx>() {{
            add(new OrderByColumnIdx(0, false));
        }};
        stmt.limit(2);

        // result ist from 1 shard, limit is 2; order by first column
        SQLReduceJobStatus status = new SQLReduceJobStatus(stmt, 1,
            new HashMap<String, AggFunction>()
        );
        List<GroupByRow> rows = new ArrayList<>();
        rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
                new ArrayList<AggState>(1) {{
                    add(new CountAggState() {{ value = 3; }});
                }}));
        rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
                new ArrayList<AggState>(1) {{
                    add(new CountAggState() {{ value = 2; }});
                }}));
        rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
                new ArrayList<AggState>(1) {{
                    add(new CountAggState() {{ value = 45; }});
                }}));
        rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
                new ArrayList<AggState>(1) {{
                    add(new CountAggState() {{ value = 8; }});
                }}));
        rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
                new ArrayList<AggState>(1) {{
                    add(new CountAggState() {{ value = 40; }});
                }}));
        List<GroupByRow> sortedRows = new ArrayList<>(status.trimRows(rows));

        assertEquals(2, sortedRows.size());
        assertEquals(45L, sortedRows.get(0).aggStates.get(0).value());
        assertEquals(40L, sortedRows.get(1).aggStates.get(0).value());
    }
}
