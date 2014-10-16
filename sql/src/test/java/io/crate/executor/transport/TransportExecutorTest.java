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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.analyze.where.WhereClause;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.elasticsearch.*;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.operator.AndOperator;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.operator.OrOperator;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.DateTruncFunction;
import io.crate.planner.Plan;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.ESUpdateNode;
import io.crate.planner.node.dql.*;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.test.integration.CrateTestCluster;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class TransportExecutorTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private ClusterService clusterService;
    private ClusterName clusterName;
    private TransportExecutor executor;
    private DocSchemaInfo docSchemaInfo;

    TableIdent table = new TableIdent(null, "characters");
    Reference id_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(table, "id"), RowGranularity.DOC, DataTypes.INTEGER));
    Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(table, "name"), RowGranularity.DOC, DataTypes.STRING));
    Reference version_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(table, "_version"), RowGranularity.DOC, DataTypes.LONG));

    TableIdent partedTable = new TableIdent(null, "parted");
    Reference parted_id_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(partedTable, "id"), RowGranularity.DOC, DataTypes.INTEGER));
    Reference parted_name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(partedTable, "name"), RowGranularity.DOC, DataTypes.STRING));
    Reference parted_date_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(partedTable, "date"), RowGranularity.DOC, DataTypes.TIMESTAMP));

    @Before
    public void transportSetUp() {
        CrateTestCluster cluster = cluster();
        clusterService = cluster.getInstance(ClusterService.class);
        clusterName = cluster.getInstance(ClusterName.class);
        executor = cluster.getInstance(TransportExecutor.class);

        docSchemaInfo = cluster.getInstance(DocSchemaInfo.class);
    }

    private void insertCharacters() {
        execute("create table characters (id int primary key, name string)");
        ensureGreen();
        execute("insert into characters (id, name) values (1, 'Arthur')");
        execute("insert into characters (id, name) values (2, 'Ford')");
        execute("insert into characters (id, name) values (3, 'Trillian')");
        refresh();
    }

    private void createPartitionedTable() {
        execute("create table parted (id int, name string, date timestamp) partitioned by (date)");
        ensureGreen();
        execute("insert into parted (id, name, date) values (?, ?, ?), (?, ?, ?), (?, ?, ?)",
                new Object[]{
                        1, "Trillian", null,
                        2, null, 0L,
                        3, "Ford", 1396388720242L
                });
        ensureGreen();
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
        collectNode.toCollect(Arrays.asList(reference));
        collectNode.outputTypes(asList(load1.type()));
        collectNode.maxRowGranularity(RowGranularity.NODE);

        Plan plan = new Plan();
        plan.add(collectNode);
        Job job = executor.newJob(plan);

        List<ListenableFuture<TaskResult>> result = executor.execute(job);

        assertThat(result.size(), is(2));
        for (ListenableFuture<TaskResult> nodeResult : result) {
            assertEquals(1, nodeResult.get().rows().length);
            assertThat((Double) nodeResult.get().rows()[0][0], is(greaterThan(0.0)));

        }
    }

    @Test
    public void testMapSideCollectTask() throws Exception {
        ReferenceInfo clusterNameInfo = SysClusterTableInfo.INFOS.get(new ColumnIdent("name"));
        Symbol reference = new Reference(clusterNameInfo);

        CollectNode collectNode = new CollectNode("lcollect", new Routing());
        collectNode.toCollect(asList(reference, Literal.newLiteral(2.3f)));
        collectNode.outputTypes(asList(clusterNameInfo.type()));
        collectNode.maxRowGranularity(RowGranularity.CLUSTER);

        Plan plan = new Plan();
        plan.add(collectNode);
        Job job = executor.newJob(plan);

        List<ListenableFuture<TaskResult>> results = executor.execute(job);
        assertThat(results.size(), is(1));
        Object[][] result = results.get(0).get().rows();
        assertThat(result.length, is(1));
        assertThat(result[0].length, is(2));

        assertThat(((BytesRef)result[0][0]).utf8ToString(), is(clusterName.value()));
        assertThat((Float)result[0][1], is(2.3f));
    }

    @Test
    public void testESGetTask() throws Exception {
        insertCharacters();

        ESGetNode node = new ESGetNode("characters", "2", "2");
        node.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertThat((String) objects[0][1], is("Ford"));
    }

    @Test
    public void testESGetTaskWithDynamicReference() throws Exception {
        insertCharacters();

        ESGetNode node = new ESGetNode("characters", "2", "2");
        node.outputs(ImmutableList.<Symbol>of(id_ref, new DynamicReference(
                new ReferenceIdent(new TableIdent(null, "characters"), "foo"), RowGranularity.DOC)));
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertNull(objects[0][1]);
    }

    @Test
    public void testESMultiGet() throws Exception {
        insertCharacters();
        ESGetNode node = new ESGetNode("characters", asList("1", "2"), asList("1", "2"));
        node.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(2));
    }

    @Test
    public void testESSearchTask() throws Exception {
        insertCharacters();

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode node = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(id_ref, name_ref),
                Arrays.<Symbol>asList(name_ref),
                new boolean[]{false},
                new Boolean[] { null },
                null, null, WhereClause.MATCH_ALL,
                null
        );
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        QueryThenFetchTask task = (QueryThenFetchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get().rows();
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
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Ford")));

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");
        QueryThenFetchNode node = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(id_ref, name_ref),
                Arrays.<Symbol>asList(name_ref),
                new boolean[]{false},
                new Boolean[] { null },
                null, null,
                new WhereClause(whereClause),
                null
        );
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        QueryThenFetchTask task = (QueryThenFetchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get().rows();
        assertThat(rows.length, is(1));

        assertThat((Integer) rows[0][0], is(2));
        assertThat((String) rows[0][1], is("Ford"));
    }

    @Test
    public void testESSearchTaskWithFunction() throws Exception {
        execute("create table searchf (id int primary key, date timestamp) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into searchf (id, date) values (1, '1980-01-01'), (2, '1980-01-02')");
        refresh();

        Reference id_ref = new Reference(new ReferenceInfo(
                new ReferenceIdent(
                        new TableIdent(DocSchemaInfo.NAME, "searchf"),
                        "id"),
                RowGranularity.DOC,
                DataTypes.INTEGER
        ));
        Reference date_ref = new Reference(new ReferenceInfo(
                new ReferenceIdent(
                        new TableIdent(DocSchemaInfo.NAME, "searchf"),
                        "date"),
                RowGranularity.DOC,
                DataTypes.TIMESTAMP
        ));
        Function function = new Function(new FunctionInfo(
                new FunctionIdent(DateTruncFunction.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.TIMESTAMP)),
                DataTypes.TIMESTAMP
        ), Arrays.<Symbol>asList(Literal.newLiteral("day"), new InputColumn(1)));
        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.INTEGER)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(id_ref, Literal.newLiteral(2))
        );

        DocTableInfo searchf = docSchemaInfo.getTableInfo("searchf");
        QueryThenFetchNode node = new QueryThenFetchNode(
                searchf.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(id_ref, date_ref),
                Arrays.<Symbol>asList(id_ref),
                new boolean[]{false},
                new Boolean[] { null },
                null, null,
                new WhereClause(whereClause),
                null
        );
        MergeNode mergeNode = new MergeNode("merge", 1);
        mergeNode.inputTypes(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.TIMESTAMP));
        mergeNode.outputTypes(Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.TIMESTAMP));
        TopNProjection topN = new TopNProjection(2, TopN.NO_OFFSET);
        topN.outputs(Arrays.asList(new InputColumn(0), function));
        mergeNode.projections(Arrays.<Projection>asList(topN));
        Plan plan = new Plan();
        plan.add(node);
        plan.add(mergeNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().size(), is(2));

        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] rows = result.get(0).get().rows();
        assertThat(rows.length, is(1));

        assertThat((Integer) rows[0][0], is(2));
        assertEquals(315619200000L, rows[0][1]);

    }

    @Test
    public void testESSearchTaskPartitioned() throws Exception {
        createPartitionedTable();
        // get partitions
        ImmutableOpenMap<String, List<AliasMetaData>> aliases =
                client().admin().indices().prepareGetAliases().addAliases("parted")
                        .execute().actionGet().getAliases();

        DocTableInfo parted = docSchemaInfo.getTableInfo("parted");
        QueryThenFetchNode node = new QueryThenFetchNode(
                parted.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(parted_id_ref, parted_name_ref, parted_date_ref),
                Arrays.<Symbol>asList(name_ref),
                new boolean[]{false},
                new Boolean[] { null },
                null, null,
                WhereClause.MATCH_ALL,
                Arrays.asList(parted_date_ref.info())
        );
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        QueryThenFetchTask task = (QueryThenFetchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get().rows();
        assertThat(rows.length, is(3));

        assertThat((Integer) rows[0][0], is(3));
        assertThat((String)rows[0][1], is("Ford"));
        assertThat((Long) rows[0][2], is(1396388720242L));

        assertThat((Integer) rows[1][0], is(1));
        assertThat((String) rows[1][1], is("Trillian"));
        assertNull(rows[1][2]);

        assertThat((Integer) rows[2][0], is(2));
        assertNull(rows[2][1]);
        assertThat((Long) rows[2][2], is(0L));
    }

    @Test
    public void testESDeleteByQueryTask() throws Exception {
        insertCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(id_ref, Literal.newLiteral(2)));

        ESDeleteByQueryNode node = new ESDeleteByQueryNode(
                new String[]{"characters"},
                new WhereClause(whereClause));
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        ESDeleteByQueryTask task = (ESDeleteByQueryTask) job.tasks().get(0);

        task.start();
        TaskResult taskResult = task.result().get(0).get();
        Object[][] rows = taskResult.rows();
        assertThat(rows.length, is(0));
        assertThat(taskResult.rowCount(), is(-1L));

        // verify deletion
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");
        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(id_ref, name_ref),
                Arrays.<Symbol>asList(name_ref),
                new boolean[]{false},
                new Boolean[] { null },
                null, null,
                new WhereClause(whereClause),
                null
        );
        plan = new Plan();
        plan.add(searchNode);
        job = executor.newJob(plan);
        QueryThenFetchTask searchTask = (QueryThenFetchTask) job.tasks().get(0);

        searchTask.start();
        rows = searchTask.result().get(0).get().rows();
        assertThat(rows.length, is(0));
    }

    @Test
    public void testESDeleteTask() throws Exception {
        insertCharacters();

        ESDeleteNode node = new ESDeleteNode("characters", "2", "2", Optional.<Long>absent());
        Plan plan = new Plan();
        plan.add(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] rows = taskResult.rows();
        assertThat(rows.length, is(0));
        assertThat(taskResult.rowCount(), is(1L));

        // verify deletion
        ESGetNode getNode = new ESGetNode("characters", "2", "2");
        getNode.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        plan = new Plan();
        plan.add(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(0));
    }

    @Test
    public void testESIndexTask() throws Exception {
        execute("create table characters (id int primary key, name string)");
        ensureGreen();


        XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
        builder.field(id_ref.info().ident().columnIdent().name(), 99);
        builder.field(name_ref.info().ident().columnIdent().name(), "Marvin");

        ESIndexNode indexNode = new ESIndexNode(
                new String[]{"characters"},
                Arrays.asList(builder.bytes()),
                ImmutableList.of("99"),
                ImmutableList.of("99"),
                false,
                false
        );
        Plan plan = new Plan();
        plan.add(indexNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESIndexTask.class));

        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] rows = taskResult.rows();
        assertThat(rows.length, is(0));
        assertThat(taskResult.rowCount(), is(1L));


        // verify insertion
        ESGetNode getNode = new ESGetNode("characters", "99", "99");
        getNode.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        plan = new Plan();
        plan.add(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer)objects[0][0], is(99));
        assertThat((String)objects[0][1], is("Marvin"));
    }

    @Test
    public void testESIndexPartitionedTableTask() throws Exception {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp" +
                ") partitioned by (date)");
        ensureGreen();
        Map<String, Object> sourceMap = new MapBuilder<String, Object>()
                .put("id", 0L)
                .put("name", "Trillian")
                .map();
        BytesReference source = XContentFactory.jsonBuilder().map(sourceMap).bytes();
        PartitionName partitionName = new PartitionName("parted", Arrays.asList(new BytesRef("13959981214861")));
        ESIndexNode indexNode = new ESIndexNode(
                new String[]{partitionName.stringValue()},
                Arrays.asList(source),
                ImmutableList.of("123"),
                ImmutableList.of("123"),
                true,
                false
                );
        Plan plan = new Plan();
        plan.add(indexNode);
        plan.expectsAffectedRows(true);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESIndexTask.class));
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] indexResult = taskResult.rows();
        assertThat(indexResult.length, is(0));
        assertThat(taskResult.rowCount(), is(1L));

        refresh();

        assertTrue(
                client().admin().indices().prepareExists(partitionName.stringValue())
                        .execute().actionGet().isExists()
        );
        assertTrue(
                client().admin().indices().prepareAliasesExist("parted")
                        .execute().actionGet().exists()
        );
        SearchHits hits = client().prepareSearch(partitionName.stringValue())
                .setTypes(Constants.DEFAULT_MAPPING_TYPE)
                .addFields("id", "name")
                .setQuery(new MapBuilder<String, Object>()
                                .put("match_all", new HashMap<String, Object>())
                                .map()
                ).execute().actionGet().getHits();
        assertThat(hits.getTotalHits(), is(1L));
        assertThat((Integer) hits.getHits()[0].field("id").getValues().get(0), is(0));
        assertThat((String)hits.getHits()[0].field("name").getValues().get(0), is("Trillian"));
    }

    @Test
    public void testESCountTask() throws Exception {
        insertCharacters();
        Plan plan = new Plan();
        WhereClause whereClause = new WhereClause(null, false);
        plan.add(new ESCountNode(docSchemaInfo.getTableInfo("characters"), whereClause));

        List<ListenableFuture<TaskResult>> result = executor.execute(executor.newJob(plan));
        Object[][] rows = result.get(0).get().rows();

        assertThat(rows.length, is(1));
        assertThat((Long)rows[0][0], is(3L));
    }

    @Test
    public void testESBulkInsertTask() throws Exception {
        execute("create table characters (id int primary key, name string)");
        ensureGreen();

        Map<String, Object> sourceMap1 = new HashMap<>();
        sourceMap1.put(id_ref.info().ident().columnIdent().name(), 99);
        sourceMap1.put(name_ref.info().ident().columnIdent().name(), "Marvin");
        BytesReference source1 = XContentFactory.jsonBuilder().map(sourceMap1).bytes();

        Map<String, Object> sourceMap2 = new HashMap<>();
        sourceMap2.put(id_ref.info().ident().columnIdent().name(), 42);
        sourceMap2.put(name_ref.info().ident().columnIdent().name(), "Deep Thought");
        BytesReference source2 = XContentFactory.jsonBuilder().map(sourceMap2).bytes();

        ESIndexNode indexNode = new ESIndexNode(
                new String[]{"characters"},
                Arrays.asList(source1, source2),
                ImmutableList.of("99", "42"),
                ImmutableList.of("99", "42"),
                false,
                false
        );

        Plan plan = new Plan();
        plan.add(indexNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESBulkIndexTask.class));

        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        assertThat(taskResult.rowCount(), is(2L));
        Object[][] rows = result.get(0).get().rows();
        assertThat(rows.length, is(0));

        // verify insertion

        ESGetNode getNode = new ESGetNode("characters",
                Arrays.asList("99", "42"),
                Arrays.asList("99", "42"));
        getNode.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        plan = new Plan();
        plan.add(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(2));
        assertThat((Integer)objects[0][0], is(99));
        assertThat((String)objects[0][1], is("Marvin"));

        assertThat((Integer)objects[1][0], is(42));
        assertThat((String)objects[1][1], is("Deep Thought"));
    }

    @Test
    public void testESUpdateByIdTask() throws Exception {
        insertCharacters();

        // update characters set name='Vogon lyric fan' where id=1
        WhereClause whereClause = new WhereClause(null, false);
        whereClause.clusteredByLiteral(Literal.newLiteral("1"));
        ESUpdateNode updateNode = new ESUpdateNode(
                new String[]{"characters"},
                new HashMap<Reference, Symbol>(){{
                    put(name_ref, Literal.newLiteral("Vogon lyric fan"));
                }},
                whereClause,
                asList("1"),
                asList("1")
        );
        Plan plan = new Plan();
        plan.add(updateNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESUpdateByIdTask.class));
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] rows = taskResult.rows();

        assertThat(rows.length, is(0));
        assertThat(taskResult.rowCount(), is(1L));

        // verify update
        ESGetNode getNode = new ESGetNode("characters", Arrays.asList("1"), Arrays.asList("1"));
        getNode.outputs(ImmutableList.<Symbol>of(id_ref, name_ref));
        plan = new Plan();
        plan.add(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer)objects[0][0], is(1));
        assertThat((String)objects[0][1], is("Vogon lyric fan"));
    }

    @Test
    public void testUpdateByQueryTaskWithVersion() throws Exception {
        insertCharacters();

        // do update
        Function whereClauseFunction = new Function(AndOperator.INFO, Arrays.<Symbol>asList(
                new Function(new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.LONG, DataTypes.LONG)),
                        DataTypes.BOOLEAN),
                        Arrays.<Symbol>asList(version_ref, Literal.newLiteral(1L))
                ),
                new Function(new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME,
                                Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                        DataTypes.BOOLEAN),
                        Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Arthur"))
                )));

        // update characters set name='mostly harmless' where name='Arthur' and "_version"=?
        WhereClause whereClause = new WhereClause(whereClauseFunction);
        whereClause.version(1L);
        ESUpdateNode updateNode = new ESUpdateNode(
                new String[]{"characters"},
                new HashMap<Reference, Symbol>(){{
                    put(name_ref, Literal.newLiteral("mostly harmless"));
                }},
                whereClause,
                ImmutableList.<String>of(),
                ImmutableList.<String>of()
        );
        Plan plan = new Plan();
        plan.add(updateNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESUpdateByQueryTask.class));
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        assertThat(result.get(0).get().errorMessage(), is(nullValue()));
        assertThat(result.get(0).get().rowCount(), is(1L));

        ESGetNode getNode = new ESGetNode("characters", "1", "1");
        getNode.outputs(Arrays.<Symbol>asList(id_ref, name_ref, version_ref));
        plan = new Plan();
        plan.add(getNode);
        plan.expectsAffectedRows(false);

        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] rows = result.get(0).get().rows();

        assertThat(rows.length, is(1));
        assertThat((Integer)rows[0][0], is(1));
        assertThat((String)rows[0][1], is("mostly harmless"));
        assertThat((Long)rows[0][2], is(2L));
    }

    @Test
    public void testUpdateByQueryTask() throws Exception {
        insertCharacters();

        Function whereClause = new Function(OrOperator.INFO, Arrays.<Symbol>asList(
                new Function(new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                        DataTypes.BOOLEAN),
                        Arrays.<Symbol>asList(name_ref, Literal.newLiteral("Trillian"))
                ),
                new Function(new FunctionInfo(
                        new FunctionIdent(EqOperator.NAME,
                                Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.INTEGER)), DataTypes.BOOLEAN),
                        Arrays.<Symbol>asList(id_ref, Literal.newLiteral(1))
                )));

        // update characters set name='mostly harmless' where id=1 or name='Trillian'
        ESUpdateNode updateNode = new ESUpdateNode(
                new String[]{"characters"},
                new HashMap<Reference, Symbol>(){{
                    put(name_ref, Literal.newLiteral("mostly harmless"));
                }},
                new WhereClause(whereClause),
                new ArrayList<String>(0),
                new ArrayList<String>(0)
        );
        Plan plan = new Plan();
        plan.add(updateNode);
        plan.expectsAffectedRows(true);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESUpdateByQueryTask.class));
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        assertThat(result.get(0).get().rowCount(), is(2L));

        refresh();

        // verify update
        Function searchWhereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(name_ref, Literal.newLiteral("mostly harmless")));

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");
        QueryThenFetchNode node = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(id_ref, name_ref),
                ImmutableList.<Symbol>of(id_ref),
                new boolean[]{false},
                new Boolean[] { null },
                null, null, new WhereClause(searchWhereClause),
                null
        );
        node.outputTypes(Arrays.asList(id_ref.info().type(), name_ref.info().type()));
        plan = new Plan();
        plan.add(node);
        plan.expectsAffectedRows(false);

        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] rows = result.get(0).get().rows();

        assertThat(rows.length, is(2));
        assertThat((Integer)rows[0][0], is(1));
        assertThat((String)rows[0][1], is("mostly harmless"));

        assertThat((Integer)rows[1][0], is(3));
        assertThat((String)rows[1][1], is("mostly harmless"));

    }
}
