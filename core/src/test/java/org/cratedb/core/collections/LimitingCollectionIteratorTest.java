package org.cratedb.core.collections;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

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
}
