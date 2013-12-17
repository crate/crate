package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.AggState;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.groupby.key.GlobalRows;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.stubs.HitchhikerMocks;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
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

        GlobalRows gRows = new GlobalRows(1, stmt);
        SQLReduceJobStatus status = new SQLReduceJobStatus(
            stmt, new ThreadPool(),
            1,
            null,
            null
        );

        // TODO:

        List<GroupByRow> rows = new ArrayList<>();
        // gRows.buckets()[0] = rows;
        // SQLGroupByResult result = new SQLGroupByResult(0, gRows);
        // rows.add(new GroupByRow(
        //         new GroupByKey(new Object[]{ 1}),
        //         new ArrayList<AggState>(1) {{
        //             add(new CountAggState() {{ value = 3; }});
        //         }}, stmt));
        // rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
        //         new ArrayList<AggState>(1) {{
        //             add(new CountAggState() {{ value = 2; }});
        //         }}, stmt));
        // rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
        //         new ArrayList<AggState>(1) {{
        //             add(new CountAggState() {{ value = 45; }});
        //         }}, stmt));
        // rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
        //         new ArrayList<AggState>(1) {{
        //             add(new CountAggState() {{ value = 8; }});
        //         }}, stmt));
        // rows.add(new GroupByRow(new GroupByKey(new Object[]{ 1}),
        //         new ArrayList<AggState>(1) {{
        //             add(new CountAggState() {{ value = 40; }});
        //         }}, stmt));

        // status.merge(result);
        // List<GroupByRow> sortedRows = new ArrayList<>(status.terminate());

        // assertEquals(2, sortedRows.size());
        // assertEquals(45L, sortedRows.get(0).aggStates.get(0).value());
        // assertEquals(40L, sortedRows.get(1).aggStates.get(0).value());
    }
}
