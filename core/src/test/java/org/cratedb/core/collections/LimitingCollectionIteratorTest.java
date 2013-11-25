package org.cratedb.core.collections;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class LimitingCollectionIteratorTest {


    @Test
    public void testLimitingCollectionIterator() {

        List<Integer> rows = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            rows.add(i);
        }

        List<Integer> newRows = new ArrayList<>();
        LimitingCollectionIterator<Integer> integers = new LimitingCollectionIterator<>(rows, 2);

        for (Integer integer : integers) {
            newRows.add(integer);
        }

        assertEquals(2, newRows.size());
    }

    @Test
    public void testLimitingCollectionIteratorIsEmptyLimitZero() {

        List<Integer> rows = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            rows.add(i);
        }

        LimitingCollectionIterator<Integer> integers = new LimitingCollectionIterator<>(rows, 0);
        assertThat(integers.isEmpty(), is(true));
    }

    @Test
    public void testLimitingCollectionIteratorIsEmptyEmptyInput() {

        List<Integer> rows = new ArrayList<>(0);

        LimitingCollectionIterator<Integer> integers = new LimitingCollectionIterator<>(rows, 10);
        assertThat(integers.isEmpty(), is(true));
    }
}
