package io.crate.operator.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.TestAggregationTask;
import io.crate.executor.task.LocalAggregationTask;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.planner.plan.AggregationNode;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.Value;
import io.crate.planner.symbol.ValueSymbol;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AggregationTest {

    private Map<String, SettableFuture<Object[][]>> nodeResults;
    protected List<ListenableFuture<Object[][]>> results;
    private Injector injector;
    protected Functions functions;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder().add(
                new MetaDataModule(),
                new AggregationImplModule()
        ).createInjector();
        functions = injector.getInstance(Functions.class);
    }


    public void setUpTestData(Object[][] data) {
        // Setup test data
        nodeResults = new HashMap<>(1);
        results = new ArrayList<>(1);
        SettableFuture f = SettableFuture.<Object[][]>create();
        f.set(data);
        nodeResults.put("node_0", f);
        results.add(f);
    }

    public Object[][] executeAggregation(String name, DataType dataType) throws Exception {
        AggregationNode aggregationNode = new AggregationNode("aggregate");

        ValueSymbol value = new Value(dataType);
        FunctionIdent fi = new FunctionIdent(name, ImmutableList.of(value.valueType()));
        Aggregation agg = new Aggregation(fi, ImmutableList.of(value), Aggregation.Step.ITER, Aggregation.Step.FINAL);

        aggregationNode.symbols(value, agg);
        aggregationNode.inputs(value);
        aggregationNode.outputs(agg);

        LocalAggregationTask task = new TestAggregationTask(aggregationNode, functions);
        task.upstreamResult(results);
        task.start();

        return task.result().get(0).get();
    }

}
