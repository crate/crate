package io.crate.executor.transport;

import io.crate.executor.Job;
import io.crate.executor.transport.task.RemoteCollectTask;
import io.crate.operator.reference.sys.NodeLoadExpression;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.plan.Routing;
import io.crate.planner.symbol.Symbol;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportExecutorTest extends SQLTransportIntegrationTest {

    private TransportCollectNodeAction transportCollectNodeAction;
    private ClusterService clusterService;

    @Before
    public void transportSetUp() {
        transportCollectNodeAction = cluster().getInstance(TransportCollectNodeAction.class);
        clusterService = cluster().getInstance(ClusterService.class);
    }

    @Test
    public void testRemoteCollectTask() {
        TransportExecutor executor = new TransportExecutor();


        Map<String, Map<String, Integer>> locations = new HashMap<>(2);

        for (DiscoveryNode discoveryNode : clusterService.state().nodes()) {
            locations.put(discoveryNode.id(), null);
        }

        Routing routing = new Routing(locations);
        Symbol reference = new Reference(NodeLoadExpression.INFO_LOAD_1);


        CollectNode collectNode = new CollectNode("collect", routing);
        collectNode.symbols(reference);
        collectNode.inputs(reference);
        collectNode.outputs(reference);

        // later created inside executor.newJob
        RemoteCollectTask task = new RemoteCollectTask(collectNode, transportCollectNodeAction);
        Job job = new Job();
        job.addTask(task);

        List result = executor.execute(job);

        assertThat(result.size(), is(2));
    }
}
