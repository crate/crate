package org.cratedb.action;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class SQLGroupByResultTest {

    /**
     * test that preSorted (by key) GroupByRows are merged correctly
     */


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

        assertThat((Long)result1Arr[1].get(1), is(4L));
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
        assertThat((Long)result1Arr[0].get(1), is(3L));
    }

    @Test
    public void testMerge3() throws Exception {
        SQLGroupByResult result1 = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(new GroupByKey(new Object[] {"k2"}), new CountAggState() {{ value = 2; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k4"}), new CountAggState() {{ value = 3; }})
        ));

        SQLGroupByResult result2 = new SQLGroupByResult(Arrays.asList(
            new GroupByRow(new GroupByKey(new Object[] {"k1"}), new CountAggState() {{ value = 2; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k3"}), new CountAggState() {{ value = 3; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k4"}), new CountAggState() {{ value = 4; }}),
            new GroupByRow(new GroupByKey(new Object[] {"k5"}), new CountAggState() {{ value = 5; }})
        ));

        result1.merge(result2);

        assertThat(result1.size(), is(5));
        GroupByRow[] result1Arr = result1.result.toArray(new GroupByRow[result1.result.size()]);
        assertThat((String)result1Arr[0].key.get(0), is("k1"));
        assertThat((String)result1Arr[1].key.get(0), is("k2"));
        assertThat((String)result1Arr[2].key.get(0), is("k3"));
        assertThat((String)result1Arr[3].key.get(0), is("k4"));
        assertThat((String)result1Arr[4].key.get(0), is("k5"));

        assertThat((Long)result1Arr[0].get(1), is(2L));
        assertThat((Long)result1Arr[1].get(1), is(2L));
        assertThat((Long)result1Arr[2].get(1), is(3L));
        assertThat((Long)result1Arr[3].get(1), is(7L));
        assertThat((Long)result1Arr[4].get(1), is(5L));
    }
}
