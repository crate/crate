package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.stubs.HitchhikerMocks;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SQLReduceJobStatusTest {

    @Test
    public void testToSortedArray() throws Exception {
        /**
         * after the reducing phase the GroupByRows are sorted and a limit is applied
         * test here that the sorting / limiting works correctly:
         */
        SQLParseService parseService = new SQLParseService(HitchhikerMocks.nodeExecutionContext());
        ParsedStatement stmt = parseService.parse(
            "select count(*) from characters group by race order by count(*) desc limit 2");

        // result ist from 1 shard, limit is 2; order by first column
        SQLReduceJobStatus status = new SQLReduceJobStatus(
            stmt, new ThreadPool(), 1, null, null);
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
