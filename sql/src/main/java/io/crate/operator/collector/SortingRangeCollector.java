package io.crate.operator.collector;

import com.google.common.collect.Ordering;
import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import org.apache.lucene.util.PriorityQueue;

import javax.annotation.Nullable;
import java.util.Comparator;

public class SortingRangeCollector implements RowCollector<Object[][]> {

    private RowPriorityQueue pq;
    private final Input<Object[]> input;

    // the cell indexes of the orderings
    private int[] orderBy;

    // true is reverse
    private boolean[] reverseFlags;

    private Comparator[] comparators;

    private final int start;
    private final int end;
    private int collected;


    class RowPriorityQueue extends PriorityQueue<Object[]> {

        public RowPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(Object[] a, Object[] b) {
            for (Comparator c : comparators) {
                if (c.compare(a, b) < 0) return true;
            }
            return false;
        }

        public Object[] result() {
            return getHeapArray();
        }

    }

    /**
     * Creates a new range collector
     *
     * @param offset the offset where the range starts
     * @param limit  the size of the range
     * @param input  the input implementation to get the values from
     */
    public SortingRangeCollector(int offset, int limit,
                                 int[] orderBy, boolean[] reverseFlags,
                                 Input<Object[]> input) {
        this.start = offset;
        this.end = start + limit;
        this.input = input;
        this.orderBy = orderBy;
        this.reverseFlags = reverseFlags;


        comparators = new Comparator[orderBy.length];
        for (int i = 0; i < orderBy.length; i++) {
            int col = orderBy[i];
            boolean reverse = reverseFlags[i];
            comparators[i] = new ColOrdering(col, reverse);
        }

    }

    class ColOrdering extends Ordering<Object[]> {

        private final int col;
        private final boolean reverse;
        private final Ordering<Comparable> ordering;

        ColOrdering(int col, boolean reverse) {
            this.col = col;
            this.reverse = reverse;

            // note, that we are reverse for the queue so this conditional is by intent
            if (reverse) {
                ordering = Ordering.natural();
            } else {
                ordering = Ordering.natural().reverse();
            }
        }

        @Override
        public int compare(@Nullable Object[] left, @Nullable Object[] right) {
            return ordering.compare((Comparable) left[col], (Comparable) right[col]);
        }
    }


    @Override
    public boolean startCollect() {
        collected = 0;
        pq = new RowPriorityQueue(end);
        return true;
    }

    @Override
    public boolean processRow() {
        collected++;
        pq.insertWithOverflow(input.value());
        return true;
    }

    @Override
    public Object[][] finishCollect() {
        int resultSize = pq.size() - start;
        Object[][] result = new Object[resultSize][];
        for (int i = pq.size() - start - 1; i >= 0; i--) {
            result[i] = pq.pop();
        }
        return result;
    }

}
