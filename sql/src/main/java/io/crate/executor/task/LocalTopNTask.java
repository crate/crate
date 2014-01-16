package io.crate.executor.task;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.Task;
import io.crate.operator.collector.PassThroughExpression;
import io.crate.operator.collector.SimpleRangeCollector;
import io.crate.planner.plan.TopNNode;
import io.crate.planner.symbol.SymbolVisitor;
import io.crate.planner.symbol.TopN;
import org.apache.lucene.util.PriorityQueue;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class LocalTopNTask implements Task<Object[][]> {

    protected final TopNNode planNode;

    private final SettableFuture<Object[][]> result;
    private final PassThroughExpression input;
    protected AtomicBoolean done = new AtomicBoolean(false);

    protected List<ListenableFuture<Object[][]>> upstreamResults;
    private SimpleRangeCollector collector;

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
            collector = new SimpleRangeCollector(symbol.offset(), symbol.limit(), input);
            return null;
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
