package io.crate.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.task.LocalAggregationTask;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.MetaDataModule;
import io.crate.operator.aggregation.impl.AggregationImplModule;
import io.crate.operator.reference.sys.NodeLoadExpression;
import io.crate.planner.plan.*;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static junit.framework.TestCase.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestJobExecutor {

    CollectNode collector;
    private Injector injector;
    private Functions functions;

    static class Context {

        private final Job job;
        public final Functions functions;
        public PlanNode previous;
        public PlanNode next;
        public Task previousTask;

        public Context(Functions functions) {
            this.job = new Job();
            this.functions = functions;
        }
    }

    @Before
    public void setUp() throws Exception {
        injector = new ModulesBuilder().add(
                new MetaDataModule(),
                new AggregationImplModule()
        ).createInjector();
        functions = injector.getInstance(Functions.class);

    }


    static class TestCollectTask implements Task<Object[][]> {

        ListeningExecutorService executor = MoreExecutors.sameThreadExecutor();

        Map<String, SettableFuture<Object[][]>> nodeResults;
        List<ListenableFuture<Object[][]>> results;

        private Routing routing;


        public TestCollectTask(CollectNode node) {
            // TODO: real futures
            Visitor v = new Visitor();
            v.processSymbols(node, null);
            generateResult();
        }

        private void generateResult() {
            nodeResults = new HashMap<>(routing.nodes().size());
            results = new ArrayList<>(routing.nodes().size());

            for (String nodeIdent : routing.nodes()) {
                SettableFuture f = SettableFuture.<Object[][]>create();
                nodeResults.put(nodeIdent, f);
                results.add(f);

            }
        }


        class Visitor extends SymbolVisitor<Void, Void> {

            @Override
            public Void visitRouting(Routing symbol, Void context) {
                Preconditions.checkArgument(routing == null, "Multiple routings are not supported");
                routing = symbol;
                for (Map.Entry<String, Map<String, Integer>> entry : routing.locations().entrySet()) {
                    Preconditions.checkArgument(entry.getValue() == null, "Shards are not supported");
                }
                return null;
            }
        }

        private Object[][] collect(String nodeId) {
            Double value;
            if (nodeId == "node1") {
                value = new Double(0.1);
            } else {
                value = new Double(0.5);
            }
            return new Object[][]{{value}};
        }

        @Override
        public void start() {
            for (final Map.Entry<String, SettableFuture<Object[][]>> entry : nodeResults.entrySet()) {
                executor.execute(new Runnable() {
                    @Override
                    public void run() {
                        entry.getValue().set(collect(entry.getKey()));
                    }
                });
                entry.getKey();
            }
        }

        @Override
        public List<ListenableFuture<Object[][]>> result() {
            return results;
        }

        @Override
        public void upstreamResult(List<ListenableFuture<Object[][]>> result) {

        }

    }

    static class FakeJobExecutor implements Executor {

        private final Functions functions;

        FakeJobExecutor(Functions functions) {
            this.functions = functions;
        }

        @Override
        public Job newJob(PlanNode node) {
            Context context = new Context(functions);
            node.accept(new ExectorPlanVisitor(), context);
            return context.job;
        }

        @Override
        public List<ListenableFuture<Object[][]>> execute(Job job) {
            Task lastTask = null;
            for (Task task : job.tasks()) {
                task.start();
                lastTask = task;
            }
            return lastTask.result();
        }

        static class ExectorPlanVisitor extends PlanVisitor<Context, Task> {

            @Override
            protected Task visitPlan(PlanNode node, Context context) {
                throw new ExecutionException("uhandled PlanNode: " + node.getClass());
            }

            @Override
            protected void visitSources(PlanNode node, Context context) {
                PlanNode lastNext = context.next;
                PlanNode previous = null;
                Task previousTask = context.previousTask;
                context.next = node;
                if (node.sources() != null) {
                    for (PlanNode source : node.sources()) {
                        previousTask = source.accept(this, context);
                        previous = source;
                    }
                }
                context.previousTask = previousTask;
                context.previous = previous;
                context.next = lastNext;
            }

            @Override
            public Task visitCollect(CollectNode node, Context context) {
                //collectFuture = doCollect(node);

                visitSources(node, context);
                System.out.println("collectNode: " + node + " previous: " + context.previous);
                TestCollectTask task = new TestCollectTask(node);
                context.job.addTask(task);
                return task;
            }

            @Override
            public Task visitAggregation(AggregationNode node, Context context) {
                visitSources(node, context);
                System.out.println("aggregationNode: " + node + " previous: " + context.previous);

                LocalAggregationTask task = new TestAggregationTask(node, context.functions);
                task.upstreamResult(context.previousTask.result());
                context.job.addTask(task);
                return task;
            }

            @Override
            public Task visitTopNNode(TopNNode node, Context context) {
                visitSources(node, context);
                System.out.println("TOPNNode: " + node + " previous: " + context.previous);
                TestTopNTask task = new TestTopNTask(node);
                task.upstreamResult(context.previousTask.result());
                context.job.addTask(task);
                return task;
            }
        }

    }

    @Test
    public void testTopN() throws Exception {
        Statement statement = SqlParser.createStatement("select sys.nodes.load['5'] from sys.nodes limit 2");

        CollectNode collectNode = new CollectNode("collect");
        // 3 nodes
        Map<String, Map<String, Integer>> locations = new HashMap<>(3);
        locations.put("node1", null);
        locations.put("node2", null);
        locations.put("node3", null);
        Routing routing = new Routing(locations);
        Symbol reference = new Reference(NodeLoadExpression.INFO_LOAD_5, routing);

        collectNode.symbols(reference, routing);
        collectNode.inputs(reference);
        collectNode.outputs(reference);

        TopNNode topNNode = new TopNNode("topn");
        topNNode.source(collectNode);

        ValueSymbol value = new Value(DataType.DOUBLE);
        TopN topN = new TopN(2, 0);

        topNNode.symbols(topN, value);
        topNNode.inputs(value);
        topNNode.outputs(value);

        // the executor should be a singleton
        FakeJobExecutor executor = new FakeJobExecutor(functions);
        Job job = executor.newJob(topNNode);
        Object[][] result = executor.execute(job).get(0).get();

        assertEquals(2, result.length);

        Set<Double> expected = ImmutableSet.of(0.1, 0.5);

        for (Object[] row : result) {
            Double v = (Double) row[0];
            assertNotNull(v);
            assert (expected.contains(v));
        }

    }

    @Test
    public void testNodeLoadAggregate() throws Exception {
        Statement statement = SqlParser.createStatement("select avg(sys.nodes.load['1']) from sys.nodes");

        CollectNode collectNode = new CollectNode("collect");
        // we pretend we have two nodes
        Map<String, Map<String, Integer>> locations = new HashMap<>(2);
        locations.put("node1", null);
        locations.put("node2", null);
        Routing routing = new Routing(locations);
        Symbol reference = new Reference(NodeLoadExpression.INFO_LOAD_1, routing);

        collectNode.symbols(reference, routing);
        collectNode.inputs(reference);
        collectNode.outputs(reference);

        AggregationNode aggregationNode = new AggregationNode("aggregate");
        aggregationNode.source(collectNode);

        ValueSymbol value = new Value(DataType.DOUBLE);
        FunctionIdent fi = new FunctionIdent("avg", ImmutableList.of(value.valueType()));
        Aggregation agg = new Aggregation(fi, ImmutableList.of(value), Aggregation.Step.ITER, Aggregation.Step.FINAL);

        aggregationNode.symbols(value, agg);
        aggregationNode.inputs(value);
        aggregationNode.outputs(agg);

        // the executor should be a singleton
        FakeJobExecutor executor = new FakeJobExecutor(functions);

        Job job = executor.newJob(aggregationNode);
        Object[][] result = executor.execute(job).get(0).get();
        assertEquals(0.3, result[0][0]);
        //System.out.println("result: " + Arrays.toString(result[0]));

        // ExecutionPlan(aggregationNode)
        //


        // example: select avg(sys.shards.size), max(sys.shards.size) from sys.shards
        // CollectAggregationTask
        // symbols = [
        //           Reference(path='sys.shards.size', valueType=LONG, granularity=SHARD)
        //           Aggregation(name="avg", toStep=PARTIAL, operand=0),
        //           Aggregation(name="max", toStep=PARTIAL, operand=0)
        //           ]
        //  inputs = [0]
        //  outputs = [1,2]

        // AggregateTask
        //  symbols = [
        //           Aggregation(name="avg", fromStep=PARTIAL, input=0, toStep=FINAL, operandType=LONG),
        //           Aggregation(name="max", fromStep=PARTIAL, input=1, toStep=FINAL, operandType=Long),
        //  inputs = [0,1]
        //  outputs = [0,1]


        // AggregateTask
        // aggregates = [Aggregation(name="avg", operandType=DOUBLE, fromStep=PARTIAL, toStep=FINAL)]


        // CollectRowsTaskDef
        // expressions=ColumnReference("sys.nodes.load1",


        // AggregateRowsTaskDef
        // inputTypes = List<DataType>
        // aggregates = Agg(input=0, name="avg", distinct=false, startStage=ITER, )

        // CollectAggregate
        // fields = [ReferenceInfo("sys.shards.id", rowGranularity=shard, valueType=int)]
        // aggregates = [Agg(input=0, name="avg", distinct=false, terminate=false, startStage=)]


        // FinalizeAggregateRowsTaskDef
        // aggregates = [Agg(input=0, name="avg", distinct=false, terminate=false)]

        // RowMergeTaskDef
        // inputs = [
        // aggregates = [Agg(input=0, name="avg", distinct=false, terminate=true, )


        // select count(distinct userid) from users
        // CollectAggregationTask
        // symbols = [
        //           Reference(path='users.userid', valueType=STRING, granularity=DOC),
        //           Aggregation(name="count", distinct=true, toStep=PARTIAL, operand=0),
        //           DistinctValues(input=0)
        //           ]
        //  inputs = [0]
        //  outputs = [1, 2]

        // AggregateTask
        //  symbols = [
        //           DistinctValues(valueType=STRING),
        //           AggState(name="count", operandType=STRING, step=PARTIAL),
        //           Aggregation(name="count", fromStep=PARTIAL, input=1, distinctValues=0, toStep=FINAL, distinct=true)
        //  inputs = [0,1]
        //  outputs = [1]


        //-----------------------------------------------------------------------------


    }
}
