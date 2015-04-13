package io.crate.executor.transport.task;

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.core.collections.ArrayBucket;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.executor.transport.merge.TransportDistributedResultAction;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.Aggregation;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Test;

import java.util.*;

import static io.crate.testing.TestingHelpers.isRow;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class DistributedMergeTaskTest extends SQLTransportIntegrationTest {

    @Test
    public void testDistributedMergeTask() throws Exception {
        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        Functions functions = cluster().getInstance(Functions.class);
        AggregationFunction countAggregation =
                (AggregationFunction)functions.get(new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()));

        TransportDistributedResultAction transportDistributedResultAction = cluster().getInstance(TransportDistributedResultAction.class);

        Set<String> nodes = new HashSet<>();
        for (DiscoveryNode discoveryNode : clusterService.state().nodes()) {
            nodes.add(discoveryNode.getId());
        }

        // select count(*), user ... group by user
        MergeNode mergeNode = new MergeNode(0, "merge1", 2);
        mergeNode.jobId(UUID.randomUUID());
        mergeNode.executionNodes(nodes);
        mergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.UNDEFINED, DataTypes.STRING));

        GroupProjection groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(new InputColumn(1, DataTypes.STRING)));
        groupProjection.values(Arrays.asList(
                new Aggregation(
                        countAggregation.info(),
                        ImmutableList.<Symbol>of(new InputColumn(0)),
                        Aggregation.Step.PARTIAL,
                        Aggregation.Step.FINAL
                )
        ));
        TopNProjection topNProjection = new TopNProjection(
                10, 0, Arrays.<Symbol>asList(new InputColumn(1)), new boolean[] { false }, new Boolean[] { null });
        topNProjection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

        mergeNode.projections(Arrays.asList(groupProjection, topNProjection));
        mergeNode.outputTypes(Arrays.<DataType>asList(DataTypes.STRING, DataTypes.LONG));

        Streamer<?>[] mapperOutputStreamer = new Streamer[] {
                DataTypes.LONG.streamer(),
                DataTypes.STRING.streamer()
        };

        NoopListener noopListener = new NoopListener();

        DistributedMergeTask task = new DistributedMergeTask(UUID.randomUUID(), transportDistributedResultAction, mergeNode);
        task.start();

        Iterator<String> iterator = nodes.iterator();
        String firstNode = iterator.next();

        DistributedResultRequest request1 = new DistributedResultRequest(mergeNode.jobId(), mapperOutputStreamer);
        request1.rows(new ArrayBucket(new Object[][] {
                new Object[] { 1L , new BytesRef("bar") },
        }));
        DistributedResultRequest request2 = new DistributedResultRequest(mergeNode.jobId(), mapperOutputStreamer);
        request2.rows(new ArrayBucket(new Object[][] {
                new Object[] { 1L, new BytesRef("bar") },
                new Object[] { 3L, new BytesRef("bar") },
                new Object[] { 3L, new BytesRef("foobar") },
        }));

        transportDistributedResultAction.mergeRows(firstNode, request1, noopListener);
        transportDistributedResultAction.mergeRows(firstNode, request2, noopListener);

        DistributedResultRequest request3 = new DistributedResultRequest(mergeNode.jobId(), mapperOutputStreamer);
        request3.rows(new ArrayBucket(new Object[][] {
                new Object[] { 10, new BytesRef("foo") },
                new Object[] { 20, new BytesRef("foo") },
        }));
        DistributedResultRequest request4 = new DistributedResultRequest(mergeNode.jobId(), mapperOutputStreamer);
        request4.rows(new ArrayBucket(new Object[][] {
                new Object[] { 10, new BytesRef("foo") },
                new Object[] { 14, new BytesRef("test") },
        }));

        String secondNode = iterator.next();
        transportDistributedResultAction.mergeRows(secondNode, request3, noopListener);
        transportDistributedResultAction.mergeRows(secondNode, request4, noopListener);

        assertThat(task.result().size(), is(2)); // 2 reducer nodes

        // results from first node
        Bucket rows = task.result().get(0).get().rows();
        assertThat(rows, IsIterableContainingInOrder.contains(
                isRow(new BytesRef("foobar"), 3L),
                isRow(new BytesRef("bar"), 5L)
        ));

        // results from second node
        rows = task.result().get(1).get().rows();
        assertThat(rows, IsIterableContainingInOrder.contains(
                isRow(new BytesRef("test"), 14L),
                isRow(new BytesRef("foo"), 40L)
        ));
    }

    class NoopListener implements ActionListener<DistributedResultResponse> {

        @Override
        public void onResponse(DistributedResultResponse distributedResultResponse) {

        }

        @Override
        public void onFailure(Throwable e) {
            System.out.println(e.toString());
        }
    }
}
