package io.crate.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.planner.plan.TopNNode;
import io.crate.planner.symbol.SymbolVisitor;
import io.crate.planner.symbol.TopN;
import org.apache.lucene.util.PriorityQueue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class LocalTopNTask implements Task<Object[][]> {

    protected final TopNNode planNode;

    private final SettableFuture<Object[][]> result;
    private final PassThroughExpression input;
    protected AtomicBoolean done = new AtomicBoolean(false);

    protected List<ListenableFuture<Object[][]>> upstreamResults;
    private SimpleCollector collector;

    // TODO: sorting
    class MyPriorityQueue extends PriorityQueue<Object[]> {

        MyPriorityQueue(int maxSize) {
            super(maxSize);
        }

        @Override
        protected boolean lessThan(Object[] a, Object[] b) {
            return true;
        }
    }

    class Visitor extends SymbolVisitor<Void, Void> {

        @Override
        public Void visitTopN(TopN symbol, Void context) {
            collector = new SimpleCollector(symbol.offset(), symbol.offset() + symbol.limit(),
                    input);
            return null;
        }
    }

    static class PassThroughExpression extends CollectExpression {

        private Object value;

        @Override
        public boolean setNextRow(Object... args) {
            this.value = args;
            return true;
        }

        @Override
        public Object value() {
            return this.value;
        }
    }

    static class SimpleCollector implements RowCollector<Object[][]> {

        private final AtomicInteger collected = new AtomicInteger();
        private final Input<Object[]> input;

        private Object[][] result;
        private final int start;
        private final int end;

        SimpleCollector(int start, int end, Input<Object[]> input) {
            this.start = start;
            this.end = end;
            this.result = new Object[end - start][];
            this.input = input;
        }

        @Override
        public boolean startCollect() {
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
                result[pos] = input.value();
            }
            return true;
        }

        @Override
        public Object[][] finishCollect() {
            return result;
        }
    }

    public LocalTopNTask(TopNNode node) {
        this.planNode = node;
        Visitor v = new Visitor();
        this.input = new PassThroughExpression();
        v.processSymbols(node, null);
        result = SettableFuture.<Object[][]>create();
    }


    protected void startCollect() {
        input.startCollect();
        done.set(!collector.startCollect());
    }

    protected void processUpstreamResult(Object[][] rows) {
        boolean stop = done.get();
        if (stop) return;
        for (Object[] row : rows) {
            stop = !input.setNextRow(row);
            if (!stop) {
                stop = !collector.processRow();
            }
            if (stop) {
                done.set(stop);
                return;
            }
        }
    }

    protected Object[][] finishCollect() {
        return collector.finishCollect();
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        upstreamResults = result;
    }


}
