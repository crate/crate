package org.cratedb.action;

import junit.framework.TestCase;
import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.action.collect.BytesRefColumnReference;
import org.cratedb.action.collect.CollectorContext;
import org.cratedb.action.collect.Expression;
import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.SQLGroupingCollector;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.AggFunction;
import org.cratedb.action.groupby.aggregate.count.CountStarAggFunction;
import org.cratedb.action.parser.ColumnReferenceDescription;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.service.SQLParseService;
import org.cratedb.stubs.HitchhikerMocks;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

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
        // TODO: check if we need this test on this level, since it is hard to setup
//        String sql =  "select count(*), gender, race from characters group by race, " +
//                "gender order by count(*) desc";
//        SQLParseService parseService = new SQLParseService(HitchhikerMocks.nodeExecutionContext());
//        ParsedStatement stmt = parseService.parse(sql);
//
////        ParsedStatement stmt = new ParsedStatement(
////            "select count(*), city, country from ... group by country, city order by count(*) desc"
////        );
//
////        AggExpr countAggExpr = new AggExpr(CountStarAggFunction.NAME, false, null);
////        List<Expression> expressions = new ArrayList<>();
////        expressions.add(new BytesRefColumnReference("country"));
////        expressions.add(new BytesRefColumnReference("city"));
//
////        stmt.groupByExpressions(expressions);
//
////        stmt.resultColumnList = Arrays.asList(
////            countAggExpr,
////            new ColumnReferenceDescription("city"),
////            new ColumnReferenceDescription("country")
////        );
////        stmt.aggregateExpressions = Arrays.asList(countAggExpr);
//
//        DummyGroupKeyLookup dummyGroupKeyLookup = new DummyGroupKeyLookup();
//        Map<String, AggFunction> aggFunctionMap = new HashMap<>();
//        aggFunctionMap.put(CountStarAggFunction.NAME, new CountStarAggFunction());
//        CollectorContext collectorContext = new CollectorContext();
//
//        // TODO lookup
//        SQLGroupingCollector collector = new SQLGroupingCollector(
//                stmt, collectorContext, aggFunctionMap, 1);
//
//        collector.collect(1);
//        collector.collect(2);
//        collector.collect(3);
//        collector.collect(4);
//
//        assertEquals(1, collector.partitionedResult.size());
//        assertThat("partitioned for 1 reducer", collector.partitionedResult.containsKey("r1"), is(true));
//
//        Map<GroupByKey, GroupByRow> result = collector.partitionedResult.get("r1");
//
//        assertThat(result.size(), is(3));
//
//        assertThat(result.containsKey(new GroupByKey(new Object[] {"austria", "bregenz"})), is(true));
//
//        assertThat(result.containsKey(new GroupByKey(new Object[] {"germany", "somecity1"})), is(true));
//        assertThat(result.containsKey(new GroupByKey(new Object[] {null, "somecity1"})), is(true));
//
//
//        GroupByRow row = result.get(new GroupByKey(new Object[] {"austria", "bregenz"}));
//        assertThat((Long)(row.aggStates.get(0).value()), is(2L));
//
//        row = result.get(new GroupByKey(new Object[] {null, "somecity1"}));
//        assertThat((Long)(row.aggStates.get(0).value()), is(1L));
//
//        row = result.get(new GroupByKey(new Object[] {"germany", "somecity1"}));
//        assertThat((Long)(row.aggStates.get(0).value()), is(1L));
    }

    class DummyGroupKeyLookup implements FieldLookup {


        private HashMap<Integer, HashMap<String, Object>> dummyValues = new HashMap<Integer, HashMap<String, Object>>() {{
            put(1, new HashMap<String, Object>() {{
                put("country", "austria");
                put("city", "bregenz");
            }});
            put(2, new HashMap<String, Object>() {{
                put("country", "austria");
                put("city", "bregenz");
            }});
            put(3, new HashMap<String, Object>() {{
                put("country", null);
                put("city", "somecity1");
            }});
            put(4, new HashMap<String, Object>() {{
                put("country", "germany");
                put("city", "somecity1");
            }});
        }};

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
