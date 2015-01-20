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
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.WhereClause;
import io.crate.executor.Job;
import io.crate.executor.QueryResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.task.join.NestedLoopTask;
import io.crate.executor.transport.task.UpdateByIdTask;
import io.crate.executor.transport.task.elasticsearch.ESBulkIndexTask;
import io.crate.executor.transport.task.elasticsearch.ESDeleteByQueryTask;
import io.crate.executor.transport.task.elasticsearch.ESIndexTask;
import io.crate.executor.transport.task.elasticsearch.QueryThenFetchTask;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.sys.SysClusterTableInfo;
import io.crate.metadata.sys.SysNodesTableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.DateTruncFunction;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.ESDeleteNode;
import io.crate.planner.node.dml.ESIndexNode;
import io.crate.planner.node.dml.UpdateByIdNode;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.dql.join.NestedLoopNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.test.integration.CrateTestCluster;
import io.crate.testing.TestingHelpers;
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
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.number.OrderingComparison.greaterThan;

@TestLogging("io.crate.operation.join:TRACE")
public class TransportExecutorTest extends BaseTransportExecutorTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    private ClusterService clusterService;
    private ClusterName clusterName;

    @Before
    public void setup() {
        CrateTestCluster cluster = cluster();
        clusterService = cluster.getInstance(ClusterService.class);
        clusterName = cluster.getInstance(ClusterName.class);
    }

    @After
    public void teardown() {
        clusterService = null;
        clusterName = null;
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

        Plan plan = new IterablePlan(collectNode);
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

        Plan plan = new IterablePlan(collectNode);
        Job job = executor.newJob(plan);

        List<ListenableFuture<TaskResult>> results = executor.execute(job);
        assertThat(results.size(), is(1));
        Object[][] result = results.get(0).get().rows();
        assertThat(result.length, is(1));
        assertThat(result[0].length, is(2));

        assertThat(((BytesRef) result[0][0]).utf8ToString(), is(clusterName.value()));
        assertThat((Float) result[0][1], is(2.3f));
    }

    @Test
    public void testESGetTask() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode node = newGetNode("characters", outputs, "2");
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertThat((String) objects[0][1], is("Ford"));
    }

    @Test
    public void testESGetTaskWithDynamicReference() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, new DynamicReference(
                new ReferenceIdent(new TableIdent(null, "characters"), "foo"), RowGranularity.DOC));
        ESGetNode node = newGetNode("characters", outputs, "2");
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(2));
        assertNull(objects[0][1]);
    }

    @Test
    public void testESMultiGet() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode node = newGetNode("characters", outputs, asList("1", "2"));
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(2));
    }

    @Test
    public void testESSearchTask() throws Exception {
        setup.setUpCharacters();

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode node = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL, null),
                Arrays.<Symbol>asList(idRef, nameRef),
                Arrays.<Symbol>asList(nameRef, idRef),
                new boolean[]{false, false},
                new Boolean[] { null, null },
                null, null, WhereClause.MATCH_ALL,
                null
        );
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        QueryThenFetchTask task = (QueryThenFetchTask) job.tasks().get(0);

        task.start();
        Object[][] rows = task.result().get(0).get().rows();
        assertThat(rows.length, is(4));

        assertThat((Integer) rows[0][0], is(1));
        assertThat((String) rows[0][1], is("Arthur"));

        assertThat((Integer) rows[1][0], is(4));
        assertThat((String) rows[1][1], is("Arthur"));

        assertThat((Integer) rows[2][0], is(2));
        assertThat((String) rows[2][1], is("Ford"));

        assertThat((Integer) rows[3][0], is(3));
        assertThat((String) rows[3][1], is("Trillian"));
    }

    @Test
    public void testESSearchTaskWithFilter() throws Exception {
        setup.setUpCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(nameRef, Literal.newLiteral("Ford")));

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");
        QueryThenFetchNode node = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef),
                Arrays.<Symbol>asList(nameRef),
                new boolean[]{false},
                new Boolean[] { null },
                null, null,
                new WhereClause(whereClause),
                null
        );
        Plan plan = new IterablePlan(node);
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
                        new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "searchf"),
                        "id"),
                RowGranularity.DOC,
                DataTypes.INTEGER
        ));
        Reference date_ref = new Reference(new ReferenceInfo(
                new ReferenceIdent(
                        new TableIdent(ReferenceInfos.DEFAULT_SCHEMA_NAME, "searchf"),
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
        topN.outputs(Arrays.<Symbol>asList(new InputColumn(0), function));
        mergeNode.projections(Arrays.<Projection>asList(topN));
        Plan plan = new IterablePlan(node, mergeNode);
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
        setup.setUpPartitionedTableWithName();
        // get partitions
        ImmutableOpenMap<String, List<AliasMetaData>> aliases =
                client().admin().indices().prepareGetAliases().addAliases("parted")
                        .execute().actionGet().getAliases();

        DocTableInfo parted = docSchemaInfo.getTableInfo("parted");
        QueryThenFetchNode node = new QueryThenFetchNode(
                parted.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(partedIdRef, partedNameRef, partedDateRef),
                Arrays.<Symbol>asList(nameRef),
                new boolean[]{false},
                new Boolean[] { null },
                null, null,
                WhereClause.MATCH_ALL,
                Arrays.asList(partedDateRef.info())
        );
        Plan plan = new IterablePlan(node);
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
        setup.setUpCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(idRef, Literal.newLiteral(2)));

        ESDeleteByQueryNode node = new ESDeleteByQueryNode(
                new String[]{"characters"},
                new WhereClause(whereClause));
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        ESDeleteByQueryTask task = (ESDeleteByQueryTask) job.tasks().get(0);

        task.start();
        TaskResult taskResult = task.result().get(0).get();
        Object[][] rows = taskResult.rows();
        assertThat(rows.length, is(1));
        assertThat(((Long) rows[0][0]), is(-1L));

        // verify deletion
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");
        QueryThenFetchNode searchNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef),
                Arrays.<Symbol>asList(nameRef),
                new boolean[]{false},
                new Boolean[] { null },
                null, null,
                new WhereClause(whereClause),
                null
        );
        plan = new IterablePlan(searchNode);
        job = executor.newJob(plan);
        QueryThenFetchTask searchTask = (QueryThenFetchTask) job.tasks().get(0);

        searchTask.start();
        rows = searchTask.result().get(0).get().rows();
        assertThat(rows.length, is(0));
    }

    @Test
    public void testESDeleteTask() throws Exception {
        setup.setUpCharacters();

        ESDeleteNode node = new ESDeleteNode("characters", "2", "2", Optional.<Long>absent());
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] rows = taskResult.rows();
        assertThat(rows.length, is(1));
        assertThat(((Long) rows[0][0]), is(1L));

        // verify deletion
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode getNode = newGetNode("characters", outputs, "2");
        plan = new IterablePlan(getNode);
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
        builder.field(idRef.info().ident().columnIdent().name(), 99);
        builder.field(nameRef.info().ident().columnIdent().name(), "Marvin");

        ESIndexNode indexNode = new ESIndexNode(
                new String[]{"characters"},
                Arrays.asList(builder.bytes()),
                ImmutableList.of("99"),
                ImmutableList.of("99"),
                false,
                false
        );
        Plan plan = new IterablePlan(indexNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESIndexTask.class));

        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] rows = taskResult.rows();
        assertThat(rows.length, is(1));
        assertThat(((Long) rows[0][0]), is(1L));


        // verify insertion
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode getNode = newGetNode("characters", outputs, "99");
        plan = new IterablePlan(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer) objects[0][0], is(99));
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
        Plan plan = new IterablePlan(indexNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESIndexTask.class));
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] indexResult = taskResult.rows();
        assertThat(indexResult.length, is(1));
        assertThat(((Long) indexResult[0][0]), is(1L));

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
        setup.setUpCharacters();
        WhereClause whereClause = new WhereClause(null, false);
        Plan plan = new IterablePlan(new ESCountNode(new String[]{"characters"}, whereClause));
        List<ListenableFuture<TaskResult>> result = executor.execute(executor.newJob(plan));
        Object[][] rows = result.get(0).get().rows();

        assertThat(rows.length, is(1));
        assertThat((Long)rows[0][0], is(4L));
    }

    @Test
    public void testESBulkInsertTask() throws Exception {
        execute("create table characters (id int primary key, name string)");
        ensureGreen();

        Map<String, Object> sourceMap1 = new HashMap<>();
        sourceMap1.put(idRef.info().ident().columnIdent().name(), 99);
        sourceMap1.put(nameRef.info().ident().columnIdent().name(), "Marvin");
        BytesReference source1 = XContentFactory.jsonBuilder().map(sourceMap1).bytes();

        Map<String, Object> sourceMap2 = new HashMap<>();
        sourceMap2.put(idRef.info().ident().columnIdent().name(), 42);
        sourceMap2.put(nameRef.info().ident().columnIdent().name(), "Deep Thought");
        BytesReference source2 = XContentFactory.jsonBuilder().map(sourceMap2).bytes();

        ESIndexNode indexNode = new ESIndexNode(
                new String[]{"characters"},
                Arrays.asList(source1, source2),
                ImmutableList.of("99", "42"),
                ImmutableList.of("99", "42"),
                false,
                false
        );

        Plan plan = new IterablePlan(indexNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(ESBulkIndexTask.class));

        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] rows = taskResult.rows();
        assertThat(((Long) rows[0][0]), is(2L));
        assertThat(rows.length, is(1));

        // verify insertion

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode getNode = newGetNode("characters", outputs, Arrays.asList("99", "42"));
        plan = new IterablePlan(getNode);
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
    public void testUpdateByIdTask() throws Exception {
        setup.setUpCharacters();

        // update characters set name='Vogon lyric fan' where id=1
        UpdateByIdNode updateNode = new UpdateByIdNode(
                "characters",
                "1",
                "1",
                new HashMap<String, Symbol>(){{
                    put(nameRef.info().ident().columnIdent().fqn(), Literal.newLiteral("Vogon lyric fan"));
                }},
                Optional.<Long>fromNullable(null)
        );
        Plan plan = new IterablePlan(updateNode);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(UpdateByIdTask.class));
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Object[][] rows = taskResult.rows();

        assertThat(rows.length, is(1));
        assertThat(((Long) rows[0][0]), is(1L));

        // verify update
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode getNode = newGetNode("characters", outputs, "1");
        plan = new IterablePlan(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Object[][] objects = result.get(0).get().rows();

        assertThat(objects.length, is(1));
        assertThat((Integer)objects[0][0], is(1));
        assertThat((String)objects[0][1], is("Vogon lyric fan"));
    }

    @Test
    public void testNestedLoopTask() throws Exception {
        setup.setUpCharacters();
        setup.setUpBooks();

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode leftNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef),
                Arrays.<Symbol>asList(nameRef, idRef),
                new boolean[]{false, true},
                new Boolean[] { null, null },
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        leftNode.outputTypes(ImmutableList.of(
                        idRef.info().type(),
                        nameRef.info().type())
        );

        DocTableInfo books = docSchemaInfo.getTableInfo("books");
        QueryThenFetchNode rightNode = new QueryThenFetchNode(
                books.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(booksIdRef, titleRef, authorRef),
                Arrays.<Symbol>asList(titleRef),
                new boolean[]{false},
                new Boolean[] { null },
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        rightNode.outputTypes(ImmutableList.of(
                        booksIdRef.info().type(),
                        titleRef.info().type(),
                        authorRef.info().type())
        );

        // SELECT c.id, c.name, b.id, b.title, b.author
        // FROM characters as c CROSS JOIN books as b
        // ORDER BY c.name, c.id, b.title

        TopNProjection projection = new TopNProjection(5, 3);
        projection.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.INTEGER),
                new InputColumn(3, DataTypes.STRING),
                new InputColumn(4, DataTypes.STRING)
        ));
        List<DataType> outputTypes = ImmutableList.of(
                idRef.info().type(),
                nameRef.info().type(),
                booksIdRef.info().type(),
                titleRef.info().type(),
                authorRef.info().type());

        NestedLoopNode node = new NestedLoopNode(leftNode, rightNode, true, 5, 3, true);
        node.outputTypes(outputTypes);
        node.projections(ImmutableList.<Projection>of(projection));

        Plan plan = new IterablePlan(node);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(NestedLoopTask.class));
        List<TaskResult> results = Futures.allAsList(executor.execute(job)).get();
        assertThat(results.size(), is(1));
        assertThat(results.get(0), instanceOf(QueryResult.class));
        QueryResult result = (QueryResult)results.get(0);
        assertThat(TestingHelpers.printedTable(result.rows()), is(
                        "1| Arthur| 3| Life, the Universe and Everything| Douglas Adams\n" +
                        "1| Arthur| 1| The Hitchhiker's Guide to the Galaxy| Douglas Adams\n" +
                        "1| Arthur| 2| The Restaurant at the End of the Universe| Douglas Adams\n" +
                        "2| Ford| 3| Life, the Universe and Everything| Douglas Adams\n" +
                        "2| Ford| 1| The Hitchhiker's Guide to the Galaxy| Douglas Adams\n"));


        // SELECT c.id, b.id, b.title, b.author, c.name
        // FROM characters as c CROSS JOIN books as b
        // ORDER BY b.title, c.name, c.id
        TopNProjection projection2 = new TopNProjection(5, 0);
        projection2.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(2, DataTypes.INTEGER),
                new InputColumn(3, DataTypes.STRING),
                new InputColumn(4, DataTypes.STRING),
                new InputColumn(1, DataTypes.STRING)
        ));
        List<DataType> outputTypes2 = ImmutableList.of(
                idRef.info().type(),
                booksIdRef.info().type(),
                titleRef.info().type(),
                authorRef.info().type(),
                nameRef.info().type());
        NestedLoopNode node2 = new NestedLoopNode(leftNode, rightNode, false, 5, 0, true);
        node2.outputTypes(outputTypes2);
        node2.projections(ImmutableList.<Projection>of(projection2));

        Plan plan2 = new IterablePlan(node2);

        Job job2 = executor.newJob(plan2);
        assertThat(job2.tasks().get(0), instanceOf(NestedLoopTask.class));
        List<TaskResult> results2 = Futures.allAsList(executor.execute(job2)).get();
        assertThat(results2.size(), is(1));
        assertThat(results2.get(0), instanceOf(QueryResult.class));
        QueryResult result2 = (QueryResult)results2.get(0);
        assertThat(TestingHelpers.printedTable(result2.rows()), is(
                        "4| 3| Life, the Universe and Everything| Douglas Adams| Arthur\n" +
                        "1| 3| Life, the Universe and Everything| Douglas Adams| Arthur\n" +
                        "2| 3| Life, the Universe and Everything| Douglas Adams| Ford\n" +
                        "3| 3| Life, the Universe and Everything| Douglas Adams| Trillian\n" +
                        "4| 1| The Hitchhiker's Guide to the Galaxy| Douglas Adams| Arthur\n"));
    }

    @Test
    public void testNestedLoopMixedSorting() throws Exception {
        setup.setUpCharacters();
        setup.setUpBooks();

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode leftNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, femaleRef),
                new boolean[]{false, true},
                new Boolean[]{null, null},
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        leftNode.outputTypes(ImmutableList.of(
                        idRef.info().type(),
                        nameRef.info().type(),
                        femaleRef.info().type())
        );

        DocTableInfo books = docSchemaInfo.getTableInfo("books");
        QueryThenFetchNode rightNode = new QueryThenFetchNode(
                books.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(authorRef),
                Arrays.<Symbol>asList(authorRef),
                new boolean[]{false},
                new Boolean[]{null},
                5,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        rightNode.outputTypes(ImmutableList.of(
                        authorRef.info().type())
        );

        // SELECT c.id, c.name, c.female, b.author
        // FROM characters as c CROSS JOIN books as b
        // ORDER BY c.name, b.author, c.female

        TopNProjection projection = new TopNProjection(5, 0);
        projection.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.BOOLEAN),
                new InputColumn(3, DataTypes.STRING)
        ));
        List<DataType> outputTypes = ImmutableList.of(
                idRef.info().type(),
                nameRef.info().type(),
                femaleRef.info().type(),
                authorRef.info().type());

        NestedLoopNode node = new NestedLoopNode(leftNode, rightNode, true, 5, 0, true);
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        Plan plan = new IterablePlan(node);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(NestedLoopTask.class));
        List<TaskResult> results = Futures.allAsList(executor.execute(job)).get();
        assertThat(results.size(), is(1));
        assertThat(results.get(0), instanceOf(QueryResult.class));
        QueryResult result = (QueryResult) results.get(0);
        assertThat(TestingHelpers.printedTable(result.rows()), is(
                        "4| Arthur| true| Douglas Adams\n" +
                        "4| Arthur| true| Douglas Adams\n" +
                        "4| Arthur| true| Douglas Adams\n" +
                        "1| Arthur| false| Douglas Adams\n" +
                        "1| Arthur| false| Douglas Adams\n"));
    }

    @Test
    public void testNestedLoopNestedCrossJoinBigLimit() throws Exception {
        setup.setUpCharacters();
        setup.setUpBooks();

        int queryOffset = 0;
        int queryLimit = 40;

        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        QueryThenFetchNode leftQtfNode = new QueryThenFetchNode(
                characters.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(idRef, nameRef, femaleRef),
                Arrays.<Symbol>asList(nameRef, femaleRef),
                new boolean[]{false, true},
                new Boolean[]{null, null},
                queryOffset + queryLimit,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        leftQtfNode.outputTypes(ImmutableList.of(
                        idRef.info().type(),
                        nameRef.info().type(),
                        femaleRef.info().type())
        );

        DocTableInfo books = docSchemaInfo.getTableInfo("books");
        QueryThenFetchNode rightQtfNode = new QueryThenFetchNode(
                books.getRouting(WhereClause.MATCH_ALL),
                Arrays.<Symbol>asList(titleRef),
                Arrays.<Symbol>asList(titleRef),
                new boolean[]{false},
                new Boolean[]{null},
                queryOffset + queryLimit,
                0,
                WhereClause.MATCH_ALL,
                null
        );
        rightQtfNode.outputTypes(ImmutableList.of(
                        authorRef.info().type())
        );

        // self join :)
        NestedLoopNode leftNestedLoopNode = new NestedLoopNode(
                leftQtfNode,
                leftQtfNode,
                true,
                TopN.NO_LIMIT,
                TopN.NO_OFFSET,
                true  // we are in a sorted query
        );
        leftNestedLoopNode.outputTypes(
                ImmutableList.<DataType>builder()
                        .addAll(leftQtfNode.outputTypes())
                        .addAll(leftQtfNode.outputTypes())
                        .build()
        );

        // SELECT c1.id, c1.name, c1.female, c2.id, c2.name, c2.female, b.title
        // FROM characters c1 CROSS JOIN characters c2 CROSS JOIN books b
        // ORDER BY c1.name, c1.female, c2.name, c2.female, b.title
        // LIMIT 40;
        NestedLoopNode node = new NestedLoopNode(leftNestedLoopNode, rightQtfNode, true, queryLimit, queryOffset, true);
        TopNProjection projection = new TopNProjection(queryLimit, queryOffset);
        projection.outputs(ImmutableList.<Symbol>of(
                new InputColumn(0, DataTypes.INTEGER),
                new InputColumn(1, DataTypes.STRING),
                new InputColumn(2, DataTypes.BOOLEAN),
                new InputColumn(3, DataTypes.INTEGER),
                new InputColumn(4, DataTypes.STRING),
                new InputColumn(5, DataTypes.BOOLEAN),
                new InputColumn(6, DataTypes.STRING)
        ));
        List<DataType> outputTypes = ImmutableList.<DataType>builder()
                .addAll(leftQtfNode.outputTypes())
                .addAll(leftQtfNode.outputTypes())
                .addAll(rightQtfNode.outputTypes())
                .build();
        node.projections(ImmutableList.<Projection>of(projection));
        node.outputTypes(outputTypes);

        List<Task> tasks = executor.newTasks(node, UUID.randomUUID());
        assertThat(tasks.size(), is(1));
        assertThat(tasks.get(0), instanceOf(NestedLoopTask.class));


        NestedLoopTask nestedLoopTask = (NestedLoopTask) tasks.get(0);

        List<ListenableFuture<TaskResult>> results = nestedLoopTask.result();
        assertThat(results.size(), is(1));

        ListenableFuture<TaskResult> nestedLoopResultFuture = results.get(0);
        nestedLoopTask.start();

        TaskResult result = nestedLoopResultFuture.get();
        assertThat(result, instanceOf(QueryResult.class));

        assertThat(result.rows().length, is(queryLimit));
        assertThat(TestingHelpers.printedTable(Arrays.copyOfRange(result.rows(), 0, 10)), is(
                "4| Arthur| true| 4| Arthur| true| Life, the Universe and Everything\n" +
                "4| Arthur| true| 4| Arthur| true| The Hitchhiker's Guide to the Galaxy\n" +
                "4| Arthur| true| 4| Arthur| true| The Restaurant at the End of the Universe\n" +
                "4| Arthur| true| 1| Arthur| false| Life, the Universe and Everything\n" +
                "4| Arthur| true| 1| Arthur| false| The Hitchhiker's Guide to the Galaxy\n" +
                "4| Arthur| true| 1| Arthur| false| The Restaurant at the End of the Universe\n" +
                "4| Arthur| true| 2| Ford| false| Life, the Universe and Everything\n" +
                "4| Arthur| true| 2| Ford| false| The Hitchhiker's Guide to the Galaxy\n" +
                "4| Arthur| true| 2| Ford| false| The Restaurant at the End of the Universe\n" +
                "4| Arthur| true| 3| Trillian| true| Life, the Universe and Everything\n"));

    }
}
