package io.crate.operator.collector;

import io.crate.operator.Input;
import io.crate.operator.RowCollector;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleRangeCollector<T> implements RowCollector<Object[][]> {

    private final AtomicInteger collected = new AtomicInteger();
    private int endPos;
    private final Object endPosMutex = new Object();


    private final Input<Object[]> input;
    private Object[][] result;
    private final int start;
    private final int end;
    private final int limit;

    public SimpleRangeCollector(int offset, int limit, Input<Object[]> input) {
        this.start = offset;
        this.limit = limit;
        this.end = start + limit;
        this.result = new Object[end - start][];
        this.input = input;
    }

    @Override
    public boolean startCollect() {
        endPos = 0;
        collected.set(0);
        return true;
    }

    @Override
    public boolean processRow() {
        int pos = collected.incrementAndGet() - 1;
        if (pos > end) {
            return false;
        } else if (pos < start) {
            return true;
        }
        if (pos != end) {
            int arrayPos = pos - start;
            result[arrayPos] = input.value();
            synchronized (endPosMutex) {
                endPos = Math.max(endPos, arrayPos);
            }
        }
        return true;
    }

    @Override
    public Object[][] finishCollect() {
        if (result.length == endPos + 1) {
            return result;
        }
        return Arrays.copyOf(result, endPos + 1);
    }
}
