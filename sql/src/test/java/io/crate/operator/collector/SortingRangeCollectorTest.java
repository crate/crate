package io.crate.operator.collector;

import junit.framework.TestCase;
import org.junit.Test;

public class SortingRangeCollectorTest extends TestCase {

    private Object[][] collect(SortingRangeCollector collector,
                               PassThroughExpression input, int iterations) {
        input.startCollect();
        assertTrue(collector.startCollect());
        for (int i = 0; i < iterations; i++) {
            input.setNextRow(i);
            if (!collector.processRow()) {
                break;
            }
        }
        Object[][] res = collector.finishCollect();
//        System.out.println("-----------------------");
//        for (int i = 0; i < res.length; i++) {
//            System.out.println("row: " + res[i][0]);
//        }

        return res;

    }


    private Object[][] collectMulti(SortingRangeCollector collector,
                                    PassThroughExpression input, int iterations) {
        input.startCollect();
        assertTrue(collector.startCollect());
        for (int i = 0; i < iterations; i++) {
            input.setNextRow(i, Integer.toString(i));
            if (!collector.processRow()) {
                break;
            }
        }
        Object[][] res = collector.finishCollect();
//        System.out.println("-----------------------");
//        for (int i = 0; i < res.length; i++) {
//            System.out.println("row: " + Arrays.toString(res[i]));
//        }

        return res;

    }


    @Test
    public void testDuplicates() throws Exception {

        Object[][] result;
        SortingRangeCollector collector;
        PassThroughExpression input = new PassThroughExpression();
        collector = new SortingRangeCollector(1, 2, new int[]{0}, new boolean[]{true}, input);
        input.startCollect();
        assertTrue(collector.startCollect());

        double[] collected = new double[]{0.5, 0.1, 0.5};

        for (double i : collected) {
            input.setNextRow(i);
            if (!collector.processRow()) {
                break;
            }
        }
        result = collector.finishCollect();
//        System.out.println("-----------------------");
//        for (int i = 0; i < result.length; i++) {
//            System.out.println("row: " + Arrays.toString(result[i]));
//        }

        // the whole result would be 0.5, 0.5, 0.1 but we have an offset of 0.1
        assertEquals(2, result.length);
        assertEquals(0.5, result[0][0]);
        assertEquals(0.1, result[1][0]);


    }

    @Test
    public void testMultiColAsc() throws Exception {

        Object[][] result;
        SortingRangeCollector collector;
        PassThroughExpression input = new PassThroughExpression();

        // order by the second (string) column
        collector = new SortingRangeCollector(0, 20, new int[]{1}, new boolean[]{false}, input);
        result = collectMulti(collector, input, 100);
        assertEquals(20, result.length);
        assertEquals(0, result[0][0]);
        assertEquals(10, result[2][0]);


    }

    @Test
    public void testMultiColDesc() throws Exception {

        Object[][] result;
        SortingRangeCollector collector;
        PassThroughExpression input = new PassThroughExpression();

        // order by the second (string) column
        collector = new SortingRangeCollector(10, 20, new int[]{1}, new boolean[]{true}, input);
        result = collectMulti(collector, input, 100);
        assertEquals(20, result.length);
        assertEquals(9, result[0][0]);
        assertEquals(89, result[1][0]);

        collector = new SortingRangeCollector(10, 120, new int[]{1}, new boolean[]{true}, input);
        result = collectMulti(collector, input, 100);
        assertEquals(90, result.length);
        assertEquals(9, result[0][0]);
        assertEquals(89, result[1][0]);
        assertEquals(0, result[89][0]);


    }


    @Test
    public void testSingleColAsc() throws Exception {


        Object[][] result;
        SortingRangeCollector collector;
        PassThroughExpression input = new PassThroughExpression();

        collector = new SortingRangeCollector(0, 20, new int[]{0}, new boolean[]{false}, input);
        result = collect(collector, input, 100);
        assertEquals(20, result.length);
        assertEquals(0, result[0][0]);
        assertEquals(19, result[19][0]);


        collector = new SortingRangeCollector(0, 20, new int[]{0}, new boolean[]{false}, input);
        result = collect(collector, input, 7);
        assertEquals(7, result.length);
        assertEquals(0, result[0][0]);
        assertEquals(6, result[6][0]);

        collector = new SortingRangeCollector(2, 20, new int[]{0}, new boolean[]{false}, input);
        result = collect(collector, input, 7);
        assertEquals(5, result.length);
        assertEquals(2, result[0][0]);
        assertEquals(6, result[4][0]);

        collector = new SortingRangeCollector(10, 10, new int[]{0}, new boolean[]{false}, input);
        result = collect(collector, input, 50);
        assertEquals(10, result.length);
        assertEquals(10, result[0][0]);
        assertEquals(19, result[9][0]);

        collector = new SortingRangeCollector(10, 10, new int[]{0}, new boolean[]{false}, input);
        result = collect(collector, input, 15);
        assertEquals(5, result.length);
        assertEquals(10, result[0][0]);
        assertEquals(14, result[4][0]);

    }

    @Test
    public void testSingleColDesc() throws Exception {


        Object[][] result;
        SortingRangeCollector collector;
        PassThroughExpression input = new PassThroughExpression();

        collector = new SortingRangeCollector(0, 20, new int[]{0}, new boolean[]{true}, input);
        result = collect(collector, input, 100);
        assertEquals(20, result.length);
        assertEquals(99, result[0][0]);
        assertEquals(80, result[19][0]);

        collector = new SortingRangeCollector(5, 10, new int[]{0}, new boolean[]{true}, input);
        result = collect(collector, input, 100);
        assertEquals(10, result.length);
        assertEquals(94, result[0][0]);
        assertEquals(85, result[9][0]);

        collector = new SortingRangeCollector(5, 10, new int[]{0}, new boolean[]{true}, input);
        result = collect(collector, input, 7);
        assertEquals(2, result.length);
        assertEquals(1, result[0][0]);
        assertEquals(0, result[1][0]);

    }

}