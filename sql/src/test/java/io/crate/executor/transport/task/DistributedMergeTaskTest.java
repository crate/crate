package io.crate.executor.transport.task;

import com.google.common.collect.ImmutableList;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.metadata.*;
import io.crate.operator.aggregation.AggregationFunction;
import io.crate.operator.aggregation.impl.CountAggregation;
import io.crate.planner.node.AggStateStreamer;
import io.crate.planner.node.MergeNode;
import io.crate.planner.node.PlanNodeStreamerVisitor;
import io.crate.planner.projection.GroupProjection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.transport.TransportService;
import org.junit.Test;

import java.util.*;

import static io.crate.metadata.Helpers.createReference;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class DistributedMergeTaskTest extends SQLTransportIntegrationTest {

    @Test
    public void testDistributedMergeTask() throws Exception {
        // ThreadPool threadPool = new ThreadPool();
        // DiscoveryNodes discoveryNodes = mock(DiscoveryNodes.class);

        // NetworkService networkService1 = new NetworkService(ImmutableSettings.EMPTY);
        // NettyTransport node1Transport = new NettyTransport(ImmutableSettings.EMPTY, threadPool, networkService1, Version.CURRENT);
        // node1Transport.start();

        // NetworkService networkService2 = new NetworkService(ImmutableSettings.EMPTY);
        // NettyTransport node2Transport = new NettyTransport(ImmutableSettings.EMPTY, threadPool, networkService2, Version.CURRENT);
        // node2Transport.start();

        // DiscoveryNode node1 = new DiscoveryNode(
        //         "node1", "node1", node1Transport.boundAddress().publishAddress(),
        //         ImmutableMap.<String, String>of(), Version.CURRENT);
        // DiscoveryNode node2 = new DiscoveryNode(
        //         "node1", "node1", node2Transport.boundAddress().publishAddress(),
        //         ImmutableMap.<String, String>of(), Version.CURRENT);

        // when(discoveryNodes.get("node1")).thenReturn(node1);
        // when(discoveryNodes.get("node2")).thenReturn(node2);

        // ClusterState clusterState = mock(ClusterState.class);
        // when(clusterState.nodes()).thenReturn(discoveryNodes);

        // ClusterService clusterService = mock(ClusterService.class);
        // when(clusterService.state()).thenReturn(clusterState);


        // TransportService transportService = new TransportService(node1Transport, threadPool);
        // transportService.start();
        // transportService.connectToNode(node1);
        // transportService.connectToNode(node2);

        TransportService transportService = cluster().getInstance(TransportService.class);
        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        Functions functions = cluster().getInstance(Functions.class);
        AggregationFunction countAggregation =
                (AggregationFunction)functions.get(new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()));

        TransportMergeNodeAction transportMergeNodeAction = cluster().getInstance(TransportMergeNodeAction.class);

        // TransportMergeNodeAction transportMergeNodeAction = new TransportMergeNodeAction(
        //         transportService,
        //         clusterService
        // );

        Set<String> nodes = new HashSet<>();
        for (DiscoveryNode discoveryNode : clusterService.state().nodes()) {
            nodes.add(discoveryNode.getId());
        }

        // select count(*), user ... group by user

        MergeNode mergeNode = new MergeNode("merge1", 2);
        mergeNode.contextId(UUID.randomUUID());
        mergeNode.executionNodes(nodes);
        mergeNode.inputTypes(Arrays.asList(null, DataType.STRING));

        GroupProjection groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(new InputColumn(1)));
        groupProjection.values(Arrays.asList(
                new Aggregation(
                        countAggregation.info().ident(),
                        ImmutableList.<Symbol>of(new InputColumn(0)),
                        Aggregation.Step.PARTIAL,
                        Aggregation.Step.FINAL
                )
        ));
        TopNProjection topNProjection = new TopNProjection(10, 0);
        topNProjection.outputs(Arrays.<Symbol>asList(new InputColumn(0), new InputColumn(1)));

        mergeNode.projections(Arrays.asList(groupProjection, topNProjection));

        DataType.Streamer<?>[] mapperOutputStreamer = new DataType.Streamer[] {
                new AggStateStreamer(countAggregation),
                DataType.STRING.streamer()
        };

        NoopListener noopListener = new NoopListener();

        DistributedMergeTask task = new DistributedMergeTask(transportMergeNodeAction, mergeNode);
        task.start();

        Iterator<String> iterator = nodes.iterator();
        String firstNode = iterator.next();

        DistributedResultRequest request1 = new DistributedResultRequest(mergeNode.contextId(), mapperOutputStreamer);
        request1.rows(new Object[][] {
                new Object[] { new CountAggregation.CountAggState() {{ value = 1; }}, new BytesRef("bar") },
        });
        DistributedResultRequest request2 = new DistributedResultRequest(mergeNode.contextId(), mapperOutputStreamer);
        request2.rows(new Object[][] {
                new Object[] { new CountAggregation.CountAggState() {{ value = 1; }}, new BytesRef("bar") },
                new Object[] { new CountAggregation.CountAggState() {{ value = 3; }}, new BytesRef("bar") },
                new Object[] { new CountAggregation.CountAggState() {{ value = 3; }}, new BytesRef("foobar") },
        });

        transportMergeNodeAction.mergeRows(firstNode, request1, noopListener);
        transportMergeNodeAction.mergeRows(firstNode, request2, noopListener);

        DistributedResultRequest request3 = new DistributedResultRequest(mergeNode.contextId(), mapperOutputStreamer);
        request3.rows(new Object[][] {
                new Object[] { new CountAggregation.CountAggState() {{ value = 10; }}, new BytesRef("foo") },
                new Object[] { new CountAggregation.CountAggState() {{ value = 20; }}, new BytesRef("foo") },
        });
        DistributedResultRequest request4 = new DistributedResultRequest(mergeNode.contextId(), mapperOutputStreamer);
        request4.rows(new Object[][] {
                new Object[] { new CountAggregation.CountAggState() {{ value = 10; }}, new BytesRef("foo") },
        });

        String secondNode = iterator.next();
        transportMergeNodeAction.mergeRows(secondNode, request3, noopListener);
        transportMergeNodeAction.mergeRows(secondNode, request4, noopListener);

        assertThat(task.result().size(), is(2)); // 2 reducer nodes

        Object[][] rows = task.result().get(0).get();
        assertThat(rows.length, is(2));

        rows = task.result().get(1).get();
        assertThat(rows.length, is(1));
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
