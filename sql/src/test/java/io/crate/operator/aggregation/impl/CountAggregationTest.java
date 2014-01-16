package io.crate.operator.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.executor.TestAggregationTask;
import io.crate.executor.task.LocalAggregationTask;
import io.crate.metadata.FunctionIdent;
import io.crate.operator.aggregation.AggregationTest;
import io.crate.planner.plan.AggregationNode;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.ValueSymbol;
import org.cratedb.DataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CountAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType) throws Exception {
        return executeAggregation("count", dataType);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionIdent fi = new FunctionIdent("count", ImmutableList.of(DataType.INTEGER));
        // Return type is fixed to Long
        assertEquals(DataType.LONG ,functions.get(fi).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        setUpTestData(new Object[][]{{0.7d}, {0.3d}});
        Object[][] result = executeAggregation(DataType.DOUBLE);

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testFloat() throws Exception {
        setUpTestData(new Object[][]{{0.7f}, {0.3f}});
        Object[][] result = executeAggregation(DataType.FLOAT);

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testInteger() throws Exception {
        setUpTestData(new Object[][]{{7}, {3}});
        Object[][] result = executeAggregation(DataType.INTEGER);

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testLong() throws Exception {
        setUpTestData(new Object[][]{{7L}, {3L}});
        Object[][] result = executeAggregation(DataType.LONG);

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testShort() throws Exception {
        setUpTestData(new Object[][]{{(short) 7}, {(short) 3}});
        Object[][] result = executeAggregation(DataType.SHORT);

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testString() throws Exception {
        setUpTestData(new Object[][]{{"Youri"}, {"Ruben"}});
        Object[][] result = executeAggregation(DataType.STRING);

        assertEquals(2L, result[0][0]);
    }

    @Test
    public void testNoInput() throws Exception {
        // aka. COUNT(*)

        setUpTestData(new Object[][]{{}, {}});

        AggregationNode aggregationNode = new AggregationNode("aggregate");

        FunctionIdent fi = new FunctionIdent("count", ImmutableList.<DataType>of());
        Aggregation agg = new Aggregation(fi, ImmutableList.<ValueSymbol>of(), Aggregation.Step.ITER, Aggregation.Step.FINAL);

        aggregationNode.symbols(agg);
        aggregationNode.outputs(agg);

        LocalAggregationTask task = new TestAggregationTask(aggregationNode, functions);
        task.upstreamResult(results);
        task.start();

        Object[][] result = task.result().get(0).get();

        assertEquals(2L, result[0][0]);
    }
}
