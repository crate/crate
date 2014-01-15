package io.crate.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.metadata.Functions;
import io.crate.operator.Input;
import io.crate.operator.InputCollectExpression;
import io.crate.operator.aggregation.AggregationCollector;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.CollectExpression;
import io.crate.planner.plan.AggregationNode;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class LocalAggregationTask implements Task<Object[][]> {

    private final AggregationNode planNode;
    private final List<Aggregation> aggregations;

    private final List<AggregationCollector> collectors;

    private final CollectExpression[] inputs;
    private List<ListenableFuture<Object[][]>> upstreamResults;

    private final SettableFuture<Object[][]> result = SettableFuture.create();
    List<ListenableFuture<Object[][]>> results = ImmutableList.of((ListenableFuture<Object[][]>) result);

    public LocalAggregationTask(AggregationNode node) {
        this.planNode = node;

        aggregations = new ArrayList<>();
        for (Symbol s : planNode.symbols()) {
            if (s.symbolType() == SymbolType.AGGREGATION) {
                aggregations.add((Aggregation) s);
            }
        }

        inputs = new CollectExpression[planNode.inputs().size()];
        for (int i = 0; i < inputs.length; i++) {
            inputs[i] = new InputCollectExpression(i);
        }

        collectors = new ArrayList<>(aggregations.size());

        for (Aggregation a : aggregations) {

            AggregationFunction impl = (AggregationFunction) Functions.get(a.functionIdent());
            Preconditions.checkNotNull(impl);
            Input[] aggInputs = new Input[a.inputs().size()];
            for (int i = 0; i < aggInputs.length; i++) {
                aggInputs[i] = inputs[planNode.inputs().indexOf(a.inputs().get(i))];
            }
            // TODO: distinct input
            if (a.fromStep() == Aggregation.Step.ITER) {
                collectors.add(new AggregationCollector(a, impl, aggInputs));

            } else {
                throw new RuntimeException("Step not implemented " + a.fromStep());
            }

        }
    }

    private void startCollect() {
        for (CollectExpression i : inputs) {
            i.startCollect();
        }
        for (AggregationCollector ac : collectors) {
            ac.startCollect();
        }
    }

    private void processUpstreamResult(Object[][] rows) {
        for (Object[] row : rows) {
            System.out.println("row: " + Arrays.toString(row));
            for (CollectExpression i : inputs) {
                i.setNextRow(row);
            }
            for (AggregationCollector ac : collectors) {
                ac.nextRow();
            }
        }
    }

    private Object[][] finishCollect() {
        Object[][] result = new Object[1][collectors.size()];
        int idx = 0;
        for (AggregationCollector ac : collectors) {
            result[0][idx] = ac.finishCollect();
        }
        return result;
    }

    @Override
    public void start() {
        startCollect();
        for (final ListenableFuture<Object[][]> f : upstreamResults) {
            f.addListener(new Runnable() {

                @Override
                public void run() {
                    Object[][] value = null;
                    try {
                        value = f.get();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (java.util.concurrent.ExecutionException e) {
                        e.printStackTrace();
                    }
                    processUpstreamResult(value);
                }
            }, MoreExecutors.sameThreadExecutor());
        }
        result.set(finishCollect());
    }

    @Override
    public List<ListenableFuture<Object[][]>> result() {
        return results;
    }

    @Override
    public void upstreamResult(List<ListenableFuture<Object[][]>> result) {
        upstreamResults = result;
    }

}
