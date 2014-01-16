package io.crate.operator.aggregation.impl;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.operator.aggregation.AggregationTest;
import org.cratedb.DataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MinimumAggregationTest extends AggregationTest {

    private Object[][] executeAggregation(DataType dataType) throws Exception {
        return executeAggregation("min", dataType);
    }

    @Test
    public void testReturnType() throws Exception {
        FunctionIdent fi = new FunctionIdent("min", ImmutableList.of(DataType.INTEGER));
        assertEquals(DataType.INTEGER ,functions.get(fi).info().returnType());
    }

    @Test
    public void testDouble() throws Exception {
        setUpTestData(new Object[][]{{0.8d}, {0.3d}});
        Object[][] result = executeAggregation(DataType.DOUBLE);

        assertEquals(0.3d, result[0][0]);
    }

    @Test
    public void testFloat() throws Exception {
        setUpTestData(new Object[][]{{0.8f}, {0.3f}});
        Object[][] result = executeAggregation(DataType.FLOAT);

        assertEquals(0.3f, result[0][0]);
    }

    @Test
    public void testInteger() throws Exception {
        setUpTestData(new Object[][]{{8}, {3}});
        Object[][] result = executeAggregation(DataType.INTEGER);

        assertEquals(3, result[0][0]);
    }

    @Test
    public void testLong() throws Exception {
        setUpTestData(new Object[][]{{8L}, {3L}});
        Object[][] result = executeAggregation(DataType.LONG);

        assertEquals(3L, result[0][0]);
    }

    @Test
    public void testShort() throws Exception {
        setUpTestData(new Object[][]{{(short) 8}, {(short) 3}});
        Object[][] result = executeAggregation(DataType.SHORT);

        assertEquals((short)3, result[0][0]);
    }

    @Test
    public void testString() throws Exception {
        setUpTestData(new Object[][]{{"Youri"}, {"Ruben"}});
        Object[][] result = executeAggregation(DataType.STRING);

        assertEquals("Ruben", result[0][0]);
    }

    @Test
    public void testUnsupportedType() throws Exception {
        setUpTestData(new Object[][]{{new Object()}});

        expectedException.expect(NullPointerException.class);
        expectedException.expectMessage("AggregationFunction implementation not found [FunctionIdent{name=min, argumentTypes=[object]}]");
        Object[][] result = executeAggregation(DataType.OBJECT);
    }
}
