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
import org.cratedb.action.sql.ParsedStatement;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SQLGroupByResultTest {

    @Test
    public void testMerge() throws Exception {

        ParsedStatement stmt = new ParsedStatement("select count(*) from dummy group by keycol");
        stmt.resultColumnList = new ArrayList<ColumnDescription>() {{
            add(new AggExpr(
                CountAggFunction.COUNT_ROWS_NAME,
                new ParameterInfo() {{ isAllColumn = true; }}
            ));
        }};
        SQLReduceJobStatus jobStatus = new SQLReduceJobStatus(stmt, 1, new HashMap<String, AggFunction>());

        GroupByKey k1 = new GroupByKey(new Object[] { "k1" });
        GroupByKey k2 = new GroupByKey(new Object[] { "k2" });
        GroupByKey k3 = new GroupByKey(new Object[] { "k3" });
        GroupByKey k4 = new GroupByKey(new Object[] { "k4" });
        GroupByKey k5 = new GroupByKey(new Object[] { "k5" });

        SQLGroupByResult result1 = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(k1,
                    new ArrayList<AggState>(1) {{
                        add(new CountAggState() {{ value = 1; }});
                    }}),
            new GroupByRow(k2,
                    new ArrayList<AggState>(1) {{
                        add(new CountAggState() {{ value = 2; }});
                    }}),
            new GroupByRow(k3,
                    new ArrayList<AggState>(1) {{
                        add(new CountAggState() {{ value = 3; }});
                    }})
        ));

        SQLGroupByResult result2 = new SQLGroupByResult(Arrays.asList(
                new GroupByRow(k2,
                        new ArrayList<AggState>(1) {{
                            add(new CountAggState() {{ value = 2; }});
                        }}),
                new GroupByRow(k3,
                        new ArrayList<AggState>(1) {{
                            add(new CountAggState() {{ value = 3; }});
                        }}),
                new GroupByRow(k4,
                        new ArrayList<AggState>(1) {{
                            add(new CountAggState() {{ value = 4; }});
                        }}),
                new GroupByRow(k5,
                        new ArrayList<AggState>(1) {{
                            add(new CountAggState() {{ value = 5; }});
                        }})
        ));

        jobStatus.merge(result1);
        jobStatus.merge(result2);

        assertThat(jobStatus.reducedResult.size(), is(5));
        jobStatus.reducedResult.containsKey(k1);
        jobStatus.reducedResult.containsKey(k2);
        jobStatus.reducedResult.containsKey(k3);
        jobStatus.reducedResult.containsKey(k4);
        jobStatus.reducedResult.containsKey(k5);

        assertThat((Long)jobStatus.reducedResult.get(k1).aggStates.get(0).value(), is(1L));
        assertThat((Long)jobStatus.reducedResult.get(k2).aggStates.get(0).value(), is(4L));
        assertThat((Long)jobStatus.reducedResult.get(k3).aggStates.get(0).value(), is(6L));
        assertThat((Long)jobStatus.reducedResult.get(k4).aggStates.get(0).value(), is(4L));
        assertThat((Long)jobStatus.reducedResult.get(k5).aggStates.get(0).value(), is(5L));
    }
}
