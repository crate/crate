package org.cratedb.action;

import junit.framework.TestCase;
import org.cratedb.action.groupby.*;
import org.cratedb.action.groupby.aggregate.AggExpr;
import org.cratedb.action.groupby.aggregate.count.CountAggFunction;
import org.cratedb.action.sql.ParsedStatement;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SQLGroupingCollectorTest extends TestCase {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }


    // TODO: fix test
    public void testGroupBySingleColumnWithCount() throws Exception {


        ParameterInfo paramInfo = new ParameterInfo();
        paramInfo.isAllColumn = true;

        AggExpr[] aggExprs = new AggExpr[] {
            new AggExpr(CountAggFunction.NAME, paramInfo)
        };

        // TODO: update test
        //SQLGroupingCollector collector = new SQLGroupingCollector(groupKeyExprs, aggExprs, new DummyKeyLookup());
        //collector.collect(1);
        //collector.collect(2);
        //collector.collect(3);
        //collector.collect(4);
        //collector.collect(5);

        //assertEquals(2, collector.partitionedResult.size());
        //assertTrue(collector.partitionedResult.containsKey("male"));
        //assertTrue(collector.partitionedResult.containsKey("female"));
        //assertEquals(2, ((CountAggState)collector.partitionedResult.get("male").get("CountAggFunction")).value);
        //assertEquals(3, ((CountAggState)collector.partitionedResult.get("female").get("CountAggFunction")).value);
    }
}
