package io.crate.executor.transport.task;

import com.google.common.collect.ImmutableList;
import io.crate.Streamer;
import io.crate.executor.transport.distributed.DistributedResultRequest;
import io.crate.executor.transport.distributed.DistributedResultResponse;
import io.crate.executor.transport.merge.TransportMergeNodeAction;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.operation.aggregation.AggregationFunction;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.planner.node.AggregationStateStreamer;
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
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class DistributedMergeTaskTest extends SQLTransportIntegrationTest {

    @Test
    public void testDistributedMergeTask() throws Exception {
        ClusterService clusterService = cluster().getInstance(ClusterService.class);
        Functions functions = cluster().getInstance(Functions.class);
        AggregationFunction countAggregation =
                (AggregationFunction)functions.get(new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()));

        TransportMergeNodeAction transportMergeNodeAction = cluster().getInstance(TransportMergeNodeAction.class);

        Set<String> nodes = new HashSet<>();
        for (DiscoveryNode discoveryNode : clusterService.state().nodes()) {
            nodes.add(discoveryNode.getId());
        }

        // select count(*), user ... group by user
        MergeNode mergeNode = new MergeNode("merge1", 2);
        mergeNode.contextId(UUID.randomUUID());
        mergeNode.executionNodes(nodes);
        mergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.UNDEFINED, DataTypes.STRING));

        GroupProjection groupProjection = new GroupProjection();
        groupProjection.keys(Arrays.<Symbol>asList(new InputColumn(1)));
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
        mergeNode.outputTypes(Arrays.<DataType>asList(DataTypes.STRING, DataTypes.UNDEFINED));

        Streamer<?>[] mapperOutputStreamer = new Streamer[] {
                new AggregationStateStreamer(countAggregation),
                DataTypes.STRING.streamer()
        };

        NoopListener noopListener = new NoopListener();


        DistributedMergeTask task = new DistributedMergeTask(UUID.randomUUID(), transportMergeNodeAction, mergeNode);
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
                new Object[] { new CountAggregation.CountAggState() {{ value = 14; }}, new BytesRef("test") },
        });

        String secondNode = iterator.next();
        transportMergeNodeAction.mergeRows(secondNode, request3, noopListener);
        transportMergeNodeAction.mergeRows(secondNode, request4, noopListener);

        assertThat(task.result().size(), is(2)); // 2 reducer nodes

        // results from first node
        Object[][] rows = task.result().get(0).get().rows();
        assertThat(rows.length, is(2));

        assertThat((BytesRef)rows[0][0], is(new BytesRef("foobar")));
        assertThat((Long)rows[0][1], is(3L));

        assertThat((BytesRef)rows[1][0], is(new BytesRef("bar")));
        assertThat((Long)rows[1][1], is(5L));




        // results from second node
        rows = task.result().get(1).get().rows();
        assertThat(rows.length, is(2));
        assertThat((BytesRef)rows[0][0], is(new BytesRef("test")));
        assertThat((Long)rows[0][1], is(14L));

        assertThat((BytesRef)rows[1][0], is(new BytesRef("foo")));
        assertThat((Long)rows[1][1], is(40L));
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
