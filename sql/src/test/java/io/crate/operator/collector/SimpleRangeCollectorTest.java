package io.crate.operator.collector;

import junit.framework.TestCase;
import org.junit.Test;

public class SimpleRangeCollectorTest extends TestCase {


    private Object[][] collect(int offset, int limit, int iterations) {
        PassThroughExpression input = new PassThroughExpression();
        SimpleRangeCollector collector = new SimpleRangeCollector(offset, limit, input);
        input.startCollect();
        assertTrue(collector.startCollect());
        for (int i = 0; i < iterations; i++) {
            input.setNextRow(i);
            if (!collector.processRow()) {
                break;
            }
        }
        return collector.finishCollect();

    }

    @Test
    public void testBiggerResult() throws Exception {
        Object[][] res = collect(0, 10, 20);
        assertEquals(10, res.length);

        res = collect(5, 10, 20);
        assertEquals(10, res.length);
        assertEquals(5, res[0][0]);

    }

    @Test
    public void testSameSizeResult() throws Exception {
        Object[][] res = collect(0, 10, 10);
        assertEquals(10, res.length);

        res = collect(5, 10, 15);
        assertEquals(10, res.length);
        assertEquals(5, res[0][0]);

    }

    @Test
    public void testSmallerResult() throws Exception {
        Object[][] res = collect(0, 10, 5);
        assertEquals(5, res.length);
        assertEquals(0, res[0][0]);
        assertEquals(4, res[4][0]);

        res = collect(5, 10, 6);
        assertEquals(1, res.length);
        assertEquals(5, res[0][0]);

    }
}
