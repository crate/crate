/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Job;
import io.crate.executor.transport.task.RemoteCollectTask;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.executor.transport.task.elasticsearch.ESSearchTask;
import io.crate.metadata.*;
import io.crate.operator.aggregation.impl.CountAggregation;
import io.crate.operator.operator.EqOperator;
import io.crate.operator.reference.sys.node.NodeLoadExpression;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.CollectNode;
import io.crate.planner.node.ESGetNode;
import io.crate.planner.node.ESSearchNode;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.action.count.TransportCountAction;
import org.elasticsearch.action.get.TransportGetAction;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportExecutorTest extends SQLTransportIntegrationTest {

    private TransportCollectNodeAction transportCollectNodeAction;
    private ClusterService clusterService;

    private TransportExecutor executor;
    private TransportGetAction transportGetAction;

    TableIdent table = new TableIdent(null, "characters");
    Reference id_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(table, "id"), RowGranularity.DOC, DataType.INTEGER));
    Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(table, "name"), RowGranularity.DOC, DataType.STRING));
    Aggregation countAgg = new Aggregation(
            new FunctionIdent(CountAggregation.NAME, ImmutableList.<DataType>of()),
            ImmutableList.<Symbol>of(),
            Aggregation.Step.ITER,
            Aggregation.Step.FINAL
    );

    @Before
    public void transportSetUp() {
        transportCollectNodeAction = cluster().getInstance(TransportCollectNodeAction.class);
        transportGetAction = cluster().getInstance(TransportGetAction.class);
        clusterService = cluster().getInstance(ClusterService.class);

        Functions functions = cluster().getInstance(Functions.class);
        TransportSearchAction transportSearchAction = cluster().getInstance(TransportSearchAction.class);
        TransportCountAction transportCountAction = cluster().getInstance(TransportCountAction.class);
        executor = new TransportExecutor(transportSearchAction, transportCountAction, functions, null);
    }

    private void insertCharacters() {
        execute("create table characters (id int primary key, name string)");
        execute("insert into characters (id, name) values (1, 'Arthur')");
        execute("insert into characters (id, name) values (2, 'Ford')");
        execute("insert into characters (id, name) values (3, 'Trillian')");
        refresh();
    }

    @Test
    public void testRemoteCollectTask() throws Exception {
        Map<String, Map<String, Set<Integer>>> locations = new HashMap<>(2);

        for (DiscoveryNode discoveryNode : clusterService.state().nodes()) {
            locations.put(discoveryNode.id(), new HashMap<String, Set<Integer>>());
        }

        Routing routing = new Routing(locations);
        Symbol reference = new Reference(NodeLoadExpression.INFO_LOAD_1);

        CollectNode collectNode = new CollectNode("collect", routing);
        collectNode.toCollect(Arrays.<Symbol>asList(reference));
        collectNode.outputTypes(Arrays.asList(NodeLoadExpression.INFO_LOAD_1.type()));

        // later created inside executor.newJob
        RemoteCollectTask task = new RemoteCollectTask(collectNode, transportCollectNodeAction);
        Job job = new Job();
        job.addTask(task);

        List<ListenableFuture<Object[][]>> result = executor.execute(job);

        assertThat(result.size(), is(2));
        for (ListenableFuture<Object[][]> nodeResult : result) {
            assertEquals(1, nodeResult.get().length);
            assertThat((Double) nodeResult.get()[0][0], is(greaterThan(0.0)));
        }
    }

    @Test
    public void testESGetTask() throws Exception {
        insertCharacters();

        ESGetNode node = new ESGetNode("characters", "2");
        node.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        ESGetTask task = new ESGetTask(transportGetAction, node);
        task.start();
        Object[][] objects = task.result().get(0).get();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertThat((String) objects[0][1], is("Ford"));
    }

    @Test
    public void testESSearchTask() throws Exception {
        insertCharacters();

        ESSearchNode node = new ESSearchNode(
                Arrays.<Symbol>asList(id_ref, name_ref),
                Arrays.<Reference>asList(name_ref),
                new boolean[]{false},
                null, null, null
        );
        Job job = executor.newJob(node);
        ESSearchTask task = (ESSearchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get();
        assertThat(rows.length, is(3));

        assertThat((Integer) rows[0][0], is(1));
        assertThat((String) rows[0][1], is("Arthur"));

        assertThat((Integer) rows[1][0], is(2));
        assertThat((String) rows[1][1], is("Ford"));

        assertThat((Integer) rows[2][0], is(3));
        assertThat((String) rows[2][1], is("Trillian"));
    }

    @Test
    public void testESSearchTaskCount() throws Exception {
        insertCharacters();
        ESSearchNode node = new ESSearchNode(
                Arrays.<Symbol>asList(countAgg),
                null,
                null,
                null,
                null,
                null
        );
        Job job = executor.newJob(node);
        ESSearchTask task = (ESSearchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get();
        assertThat(rows.length, is(1));
        assertThat((Long)rows[0][0], is(3L));
    }

    @Test
    public void testESSearchTaskWithFilter() throws Exception {
        insertCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.asList(DataType.STRING, DataType.STRING)),
                DataType.BOOLEAN),
                Arrays.<Symbol>asList(name_ref, new StringLiteral("Ford")));

        ESSearchNode node = new ESSearchNode(
                Arrays.<Symbol>asList(id_ref, name_ref),
                Arrays.<Reference>asList(name_ref),
                new boolean[]{false},
                null, null,
                whereClause
        );
        Job job = executor.newJob(node);
        ESSearchTask task = (ESSearchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get();
        assertThat(rows.length, is(1));

        assertThat((Integer) rows[0][0], is(2));
        assertThat((String) rows[0][1], is("Ford"));
    }
}
