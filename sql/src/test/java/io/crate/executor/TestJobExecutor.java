package io.crate.executor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.executor.task.LocalAggregationTask;
import io.crate.metadata.FunctionIdent;
import io.crate.operator.aggregation.impl.AverageAggregation;
import io.crate.operator.reference.sys.NodeLoadExpression;
import io.crate.planner.plan.AggregationNode;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.plan.PlanNode;
import io.crate.planner.plan.PlanVisitor;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.cratedb.DataType;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class TestJobExecutor {

    CollectNode collector;

    static class Context {

        private final Job job;
        public PlanNode previous;
        public PlanNode next;
        public Task previousTask;

        public Context() {
            this.job = new Job();
        }
    }

    static class FakeCollectTask implements Task<Object[][]> {

        ListeningExecutorService executor = MoreExecutors.sameThreadExecutor();

        Map<String, SettableFuture<Object[][]>> nodeResults;
        List<ListenableFuture<Object[][]>> results;

        private Routing routing;


        public FakeCollectTask(CollectNode node) {
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

        @Override
        public Job newJob(PlanNode node) {
            Context context = new Context();
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

        class ExectorPlanVisitor extends PlanVisitor<Context, Task> {

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
                FakeCollectTask task = new FakeCollectTask(node);
                context.job.addTask(task);
                return task;
            }

            @Override
            public Task visitAggregation(AggregationNode node, Context context) {
                visitSources(node, context);
                System.out.println("aggregationNode: " + node + " previous: " + context.previous);

                LocalAggregationTask task = new TestAggregationTask(node);
                task.upstreamResult(context.previousTask.result());
                context.job.addTask(task);
                return task;
            }
        }

    }

    @Test
    public void testNodeLoadAggregate() throws Exception {


        // TODO: register aggregation functions somewhere else
        AverageAggregation.register();

        Statement statement = SqlParser.createStatement("select avg(sys.nodes.load['1']) from sys.nodes");
        //Expression expr = SqlParser.createExpression("avg(sys.nodes.load1)");

        // select avg(load1) from sys.nodes;

//        CollectorRoutingService crs = new FakeCollectorRoutingService();
//        JobOld job = new JobOld();
//        JobOld.CollectorTask ct = new JobOld.CollectorTask(job);
//        ct.routing(crs.allNodes());


//        FunctionInfo avg = new FunctionInfo(new QualifiedName("avg"), "Average", DataType.DOUBLE,
//                ImmutableList.of(DataType.DOUBLE), true);

        // example: select avg(sys.nodes.load1) from sys.nodes
        // CollectRowsTask
        // symbols = [
        //           Reference(path='sys.nodes.load1', valueType=DOUBLE, granularity=NODE)
        //           ]
        //  inputs = [0]
        //  outputs = [0]


        CollectNode collectNode = new CollectNode("collect");

        // we pretend we have two nodes
        Map<String, Map<String, Integer>> locations = new HashMap<>(2);
        locations.put("node1", null);
        locations.put("node2", null);
        Routing routing = new Routing(locations);
        Symbol reference = new Reference(NodeLoadExpression.INFO_LOAD_1, routing);

        // TODO: check if we need the .symbols in the interface
        collectNode.symbols(reference, routing);
        collectNode.inputs(reference);
        collectNode.outputs(reference);

        // AggregateTask
        //  symbols = [
        //           Value(valueType=DOUBLE),
        //           Aggregation(name="avg", operand=0, toStep=FINAL)
        //           ]
        //  inputs = [0]
        //  outputs = [1]

        AggregationNode aggregationNode = new AggregationNode("aggregate");
        aggregationNode.source(collectNode);

        ValueSymbol value = new Value(DataType.DOUBLE);
        FunctionIdent fi = new FunctionIdent("avg", ImmutableList.of(value.valueType()));
        Aggregation agg = new Aggregation(fi, ImmutableList.of(value), Aggregation.Step.ITER, Aggregation.Step.FINAL);

        aggregationNode.symbols(value, agg);
        aggregationNode.inputs(value);
        aggregationNode.outputs(agg);

        // the executor should be a singleton
        FakeJobExecutor executor = new FakeJobExecutor();

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
