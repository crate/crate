package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SQLGroupByResultTest {


    @Test
    public void testMerge() throws Exception {

        SQLGroupByResult result1 = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(new GroupByKey(new Object[] {"k1"}), new CountAggState() {{ value = 1; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k2"}), new CountAggState() {{ value = 2; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k3"}), new CountAggState() {{ value = 3; }})
        ));

        SQLGroupByResult result2 = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(new GroupByKey(new Object[] {"k2"}), new CountAggState() {{ value = 2; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k3"}), new CountAggState() {{ value = 3; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k4"}), new CountAggState() {{ value = 4; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k5"}), new CountAggState() {{ value = 5; }})
        ));

        result1.merge(result2);

        assertThat(result1.size(), is(5));
        GroupByRow[] result1Arr = result1.result.toArray(new GroupByRow[result1.result.size()]);
        assertThat((String)result1Arr[0].key.get(0), is("k1"));
        assertThat((String)result1Arr[1].key.get(0), is("k2"));

        //assertThat((Long)result1Arr[1].aggregateStates.get(0).value(), is(4L));
    }

    @Test
    public void testMerge2() throws Exception {

        SQLGroupByResult result1 = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(new GroupByKey(new Object[] {"k1"}), new CountAggState() {{ value = 1; }})
        ));

        SQLGroupByResult result2 = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(new GroupByKey(new Object[] {"k1"}), new CountAggState() {{ value = 2; }})
        ));

        result1.merge(result2);

        assertThat(result1.size(), is(1));
        GroupByRow[] result1Arr = result1.result.toArray(new GroupByRow[result1.result.size()]);
        //assertThat((Long)result1Arr[0].aggregateStates.get(0).value(), is(3L));
    }
}
