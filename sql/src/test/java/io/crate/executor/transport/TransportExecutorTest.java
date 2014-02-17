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
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.executor.Job;
import io.crate.executor.transport.task.elasticsearch.ESBulkIndexTask;
import io.crate.executor.transport.task.elasticsearch.ESDeleteByQueryTask;
import io.crate.executor.transport.task.elasticsearch.ESIndexTask;
import io.crate.executor.transport.task.elasticsearch.ESSearchTask;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operator.operator.EqOperator;
import io.crate.planner.Plan;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.*;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;
import org.cratedb.DataType;
import org.cratedb.SQLTransportIntegrationTest;
import org.cratedb.test.integration.CrateIntegrationTest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportExecutorTest extends SQLTransportIntegrationTest {

    private ClusterService clusterService;
    private ClusterName clusterName;
    private TransportExecutor executor;

    TableIdent table = new TableIdent(null, "characters");
    Reference id_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(table, "id"), RowGranularity.DOC, DataType.INTEGER));
    Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(table, "name"), RowGranularity.DOC, DataType.STRING));


    @Before
    public void transportSetUp() {
        clusterService = cluster().getInstance(ClusterService.class);
        clusterName = cluster().getInstance(ClusterName.class);
        executor = cluster().getInstance(TransportExecutor.class);
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
        ReferenceInfo load1 = SysNodesTableInfo.INFOS.get(new ColumnIdent("load", "1"));
        Symbol reference = new Reference(load1);

        CollectNode collectNode = new CollectNode("collect", routing);
        collectNode.toCollect(Arrays.<Symbol>asList(reference));
        collectNode.outputTypes(asList(load1.type()));
        collectNode.maxRowGranularity(RowGranularity.NODE);

        Plan plan = new Plan();
        plan.add(collectNode);
        Job job = executor.newJob(plan);

        List<ListenableFuture<Object[][]>> result = executor.execute(job);

        assertThat(result.size(), is(2));
        for (ListenableFuture<Object[][]> nodeResult : result) {
            assertEquals(1, nodeResult.get().length);
            assertThat((Double) nodeResult.get()[0][0], is(greaterThan(0.0)));

        }
    }

    @Test
    public void testLocalCollectTask() throws Exception {
        ReferenceInfo clusterNameInfo = SysClusterTableInfo.INFOS.get(new ColumnIdent("name"));
        Symbol reference = new Reference(clusterNameInfo);

        CollectNode collectNode = new CollectNode("lcollect", new Routing());
        collectNode.toCollect(asList(reference, new FloatLiteral(2.3f)));
        collectNode.outputTypes(asList(clusterNameInfo.type()));
        collectNode.maxRowGranularity(RowGranularity.CLUSTER);

        Plan plan = new Plan();
        plan.add(collectNode);
        Job job = executor.newJob(plan);

        List<ListenableFuture<Object[][]>> results = executor.execute(job);
        assertThat(results.size(), is(1));
        Object[][] result = results.get(0).get();
        assertThat(result.length, is(1));
        assertThat(result[0].length, is(2));

        assertThat(((BytesRef)result[0][0]).utf8ToString(), is(clusterName.value()));
        assertThat((Float)result[0][1], is(2.3f));
    }

    @Test
    public void testESGetTask() throws Exception {
        insertCharacters();

        ESGetNode node = new ESGetNode("characters", "2");
        node.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<Object[][]>> result = executor.execute(job);
        Object[][] objects = result.get(0).get();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertThat((String) objects[0][1], is("Ford"));
    }

    @Test
    public void testESMultiGet() throws Exception {
        insertCharacters();
        ESGetNode node = new ESGetNode("characters", asList("1", "2"));
        node.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<Object[][]>> result = executor.execute(job);
        Object[][] objects = result.get(0).get();

        assertThat(objects.length, is(2));
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
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
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
    public void testESSearchTaskWithFilter() throws Exception {
        insertCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, asList(DataType.STRING, DataType.STRING)),
                DataType.BOOLEAN),
                Arrays.<Symbol>asList(name_ref, new StringLiteral("Ford")));

        ESSearchNode node = new ESSearchNode(
                Arrays.<Symbol>asList(id_ref, name_ref),
                Arrays.<Reference>asList(name_ref),
                new boolean[]{false},
                null, null,
                whereClause
        );
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        ESSearchTask task = (ESSearchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get();
        assertThat(rows.length, is(1));

        assertThat((Integer) rows[0][0], is(2));
        assertThat((String) rows[0][1], is("Ford"));
    }

    @Test
    public void testESDeleteByQueryTask() throws Exception {
        insertCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, asList(DataType.STRING, DataType.STRING)),
                DataType.BOOLEAN),
                Arrays.<Symbol>asList(id_ref, new IntegerLiteral(2)));

        ESDeleteByQueryNode node = new ESDeleteByQueryNode(ImmutableSet.<String>of("characters"), whereClause);
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        ESDeleteByQueryTask task = (ESDeleteByQueryTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get();
        assertThat(rows.length, is(0));

        // verify deletion
        ESSearchNode searchNode = new ESSearchNode(
                Arrays.<Symbol>asList(id_ref, name_ref),
                Arrays.<Reference>asList(name_ref),
                new boolean[]{false},
                null, null,
                whereClause
        );
        plan = new Plan();
        plan.add(searchNode);
        job = executor.newJob(plan);
        ESSearchTask searchTask = (ESSearchTask) job.tasks().get(0);

        searchTask.start();
        rows = searchTask.result().get(0).get();
        assertThat(rows.length, is(0));
    }

    @Test
    public void testESDeleteTask() throws Exception {
        insertCharacters();

        ESDeleteNode node = new ESDeleteNode("characters", "2");
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<Object[][]>> result = executor.execute(job);
        Object[][] rows = result.get(0).get();
        assertThat(rows.length, is(0));

        // verify deletion
        ESGetNode getNode = new ESGetNode("characters", "2");
        getNode.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        plan = new Plan();
        plan.add(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get();

        assertThat(objects.length, is(0));

    }

    @Test
    public void testESIndexTask() throws Exception {
        insertCharacters();

        ESIndexNode indexNode = new ESIndexNode("characters",
                Arrays.asList(id_ref, name_ref),
                Arrays.asList(Arrays.<Symbol>asList(
                        new IntegerLiteral(99),
                        new StringLiteral("Marvin")
                )),
                new int[]{0}
        );
        Plan plan = new Plan();
        plan.add(indexNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESIndexTask.class));

        List<ListenableFuture<Object[][]>> result = executor.execute(job);
        Object[][] rows = result.get(0).get();
        assertThat(rows.length, is(1));
        assertThat((Long)rows[0][0], is(1l));


        // verify insertion
        ESGetNode getNode = new ESGetNode("characters", "99");
        getNode.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        plan = new Plan();
        plan.add(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get();

        assertThat(objects.length, is(1));
        assertThat((Integer)objects[0][0], is(99));
        assertThat((String)objects[0][1], is("Marvin"));
    }

    @Test
    public void testESBulkInsertTask() throws Exception {
        insertCharacters();

        ESIndexNode indexNode = new ESIndexNode("characters",
                Arrays.asList(id_ref, name_ref),
                Arrays.asList(
                        Arrays.<Symbol>asList(
                                new IntegerLiteral(99),
                                new StringLiteral("Marvin")
                        ),
                        Arrays.<Symbol>asList(
                                new IntegerLiteral(42),
                                new StringLiteral("Deep Thought")
                        )
                ),
                new int[]{0}
        );
        Plan plan = new Plan();
        plan.add(indexNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESBulkIndexTask.class));

        List<ListenableFuture<Object[][]>> result = executor.execute(job);
        Object[][] rows = result.get(0).get();
        assertThat(rows.length, is(1));
        assertThat((Long)rows[0][0], is(2l));

        // verify insertion

        ESGetNode getNode = new ESGetNode("characters", Arrays.asList("99", "42"));
        getNode.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        plan = new Plan();
        plan.add(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get();

        assertThat(objects.length, is(2));
        assertThat((Integer)objects[0][0], is(99));
        assertThat((String)objects[0][1], is("Marvin"));

        assertThat((Integer)objects[1][0], is(42));
        assertThat((String)objects[1][1], is("Deep Thought"));
    }
}
