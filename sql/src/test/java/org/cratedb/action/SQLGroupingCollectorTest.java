package org.cratedb.action;

import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.action.groupby.*;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.groupby.aggregate.count.CountAggState;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.ParsedStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class SQLGroupingCollectorTest extends TestCase {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }


    @Test
    public void testGroupBySingleColumnWithCount() throws Exception {
        ParameterInfo paramInfo = new ParameterInfo();
        paramInfo.isAllColumn = true;

        ParsedStatement stmt = new ParsedStatement(
            "select count(*), city, country from ... group by country, city order by count(*) desc"
        );
        stmt.groupByColumnNames = Arrays.asList("country", "city");
        stmt.resultColumnList = Arrays.asList(
            new AggExpr(CountAggFunction.NAME, paramInfo),
            new ColumnReferenceDescription("city"),
            new ColumnReferenceDescription("country")
        );

        DummyGroupKeyLookup dummyGroupKeyLookup = new DummyGroupKeyLookup();
        Map<String, AggFunction> aggFunctionMap = new HashMap<>();
        aggFunctionMap.put(CountAggFunction.NAME, new CountAggFunction());

        SQLGroupingCollector collector = new SQLGroupingCollector(
            stmt, dummyGroupKeyLookup, aggFunctionMap, new String[] { "r1" }
        );

        collector.collect(1);
        collector.collect(2);
        collector.collect(3);
        collector.collect(4);

        assertEquals(1, collector.partitionedResult.size());
        assertThat("partitioned for 1 reducer", collector.partitionedResult.containsKey("r1"), is(true));

        Map<GroupByKey, GroupByRow> result = collector.partitionedResult.get("r1");

        assertThat(result.size(), is(9));

        assertThat(result.containsKey(new GroupByKey(new Object[] {"austria", "bregenz"})), is(true));
        assertThat(result.containsKey(new GroupByKey(new Object[] {"austria", "dornbirn"})), is(true));
        assertThat(result.containsKey(new GroupByKey(new Object[] {"austria", "hohenems"})), is(true));

        assertThat(result.containsKey(new GroupByKey(new Object[] {"germany", "somecity1"})), is(true));
        assertThat(result.containsKey(new GroupByKey(new Object[] {"germany", "somecity2"})), is(true));
        assertThat(result.containsKey(new GroupByKey(new Object[] {"germany", "somecity3"})), is(true));

        assertThat(result.containsKey(new GroupByKey(new Object[] {"switzerland", "somecity1"})), is(true));
        assertThat(result.containsKey(new GroupByKey(new Object[] {"switzerland", "somecity2"})), is(true));
        assertThat(result.containsKey(new GroupByKey(new Object[] {"switzerland", "somecity3"})), is(true));

        GroupByRow row = result.get(new GroupByKey(new Object[] {"germany", "somecity1"}));
        assertThat((Long)((CountAggState)row.get(0)).value(), is(2L));

        row = result.get(new GroupByKey(new Object[] {"austria", "bregenz"}));
        assertThat((Long)((CountAggState)row.get(0)).value(), is(2L));

        row = result.get(new GroupByKey(new Object[] {"austria", "dornbirn"}));
        assertThat((Long)((CountAggState)row.get(0)).value(), is(1L));
    }

    class DummyGroupKeyLookup implements GroupByFieldLookup {


        private ImmutableMap<Integer, ImmutableMap<String, Object[]>> dummyValues =
            ImmutableMap.<Integer, ImmutableMap<String, Object[]>>builder()
            .put(1,
                ImmutableMap.<String, Object[]>builder()
                    .put("country", new Object[] {"austria"})
                    .put("city", new Object[] {"bregenz"}).build()
            )
            .put(2,
                ImmutableMap.<String, Object[]>builder()
                    .put("country", new Object[]{"austria"})
                    .put("city", new Object[]{"bregenz", "dornbirn", "hohenems"}).build()
            )
            .put(3,
                ImmutableMap.<String, Object[]>builder()
                    .put("country", new Object[]{"germany", "switzerland"})
                    .put("city", new Object[]{"somecity1"}).build()
            )
            .put(4,
                ImmutableMap.<String, Object[]>builder()
                    .put("country", new Object[] {"germany", "switzerland"})
                    .put("city", new Object[] {"somecity1", "somecity2", "somecity3"}).build()
            ).build();

        private int currentDocId;

        @Override
        public void setNextDocId(int doc) {
            currentDocId = doc;
        }

        @Override
        public void setNextReader(AtomicReaderContext context) {
        }

        @Override
        public Object lookupField(String columnName) {
            return dummyValues.get(currentDocId).get(columnName);
        }
    }
}
