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
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Bucket;
import io.crate.executor.Job;
import io.crate.executor.RowCountResult;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.task.KillTask;
import io.crate.executor.transport.task.SymbolBasedUpsertByIdTask;
import io.crate.executor.transport.task.elasticsearch.ESDeleteByQueryTask;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.aggregation.impl.CountAggregation;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.DateTruncFunction;
import io.crate.planner.*;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dml.SymbolBasedUpsertByIdNode;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dql.*;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.projection.*;
import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.routing.operation.plain.Preference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.search.SearchHits;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.*;

import static io.crate.testing.TestingHelpers.isRow;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

public class TransportExecutorTest extends BaseTransportExecutorTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
    }

    protected ESGetNode newGetNode(String tableName, List<Symbol> outputs, String singleStringKey, int executionNodeId) {
        return newGetNode(tableName, outputs, Collections.singletonList(singleStringKey), executionNodeId);
    }

    protected ESGetNode newGetNode(String tableName, List<Symbol> outputs, List<String> singleStringKeys, int executionNodeId) {
        return newGetNode(docSchemaInfo.getTableInfo(tableName), outputs, singleStringKeys, executionNodeId);
    }

    @Test
    public void testESGetTask() throws Exception {
        setup.setUpCharacters();

        // create plan
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = new Planner.Context(clusterService());
        ESGetNode node = newGetNode("characters", outputs, "2", ctx.nextExecutionNodeId());
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);

        // validate tasks
        assertThat(job.tasks().size(), is(1));
        Task task = job.tasks().get(0);
        assertThat(task, instanceOf(ESGetTask.class));

        // execute and validate results
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, "Ford")));
    }

    @Test
    public void testESGetTaskWithDynamicReference() throws Exception {
        setup.setUpCharacters();

        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, new DynamicReference(
                new ReferenceIdent(new TableIdent(null, "characters"), "foo"), RowGranularity.DOC));
        Planner.Context ctx = new Planner.Context(clusterService());
        ESGetNode node = newGetNode("characters", outputs, "2", ctx.nextExecutionNodeId());
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, null)));
    }

    @Test
    public void testESMultiGet() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = new Planner.Context(clusterService());
        ESGetNode node = newGetNode("characters", outputs, asList("1", "2"), ctx.nextExecutionNodeId());
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket objects = result.get(0).get().rows();

        assertThat(objects.size(), is(2));
    }

    @Test
    public void testQTFTask() throws Exception {
        // select id, name from characters;
        setup.setUpCharacters();
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo docIdRefInfo = characters.getReferenceInfo(new ColumnIdent(DocSysColumns.DOCID.name()));
        List<Symbol> collectSymbols = Lists.<Symbol>newArrayList(new Reference(docIdRefInfo));
        List<Symbol> outputSymbols = Lists.<Symbol>newArrayList(idRef, nameRef);

        Planner.Context ctx = new Planner.Context(clusterService());

        CollectNode collectNode = PlanNodeBuilder.collect(
                characters,
                ctx,
                WhereClause.MATCH_ALL,
                collectSymbols,
                ImmutableList.<Projection>of(),
                null,
                Constants.DEFAULT_SELECT_LIMIT
        );
        collectNode.keepContextForFetcher(true);

        FetchProjection fetchProjection = getFetchProjection((DocTableInfo) characters, (List<Symbol>) collectSymbols, (List<Symbol>) outputSymbols, (CollectNode) collectNode, ctx);

        MergeNode localMergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(fetchProjection),
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().size(), is(1));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, containsInAnyOrder(
                isRow(1, "Arthur"),
                isRow(4, "Arthur"),
                isRow(2, "Ford"),
                isRow(3, "Trillian")
        ));
    }

    @Test
    public void testQTFTaskWithFilter() throws Exception {
        // select id, name from characters where name = 'Ford';
        setup.setUpCharacters();
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo docIdRefInfo = characters.getReferenceInfo(new ColumnIdent(DocSysColumns.DOCID.name()));
        List<Symbol> collectSymbols = Lists.<Symbol>newArrayList(new Reference(docIdRefInfo));
        List<Symbol> outputSymbols = Lists.<Symbol>newArrayList(idRef, nameRef);

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(nameRef, Literal.newLiteral("Ford")));

        Planner.Context ctx = new Planner.Context(clusterService());

        CollectNode collectNode = PlanNodeBuilder.collect(
                characters,
                ctx,
                new WhereClause(whereClause),
                collectSymbols,
                ImmutableList.<Projection>of(),
                null,
                Constants.DEFAULT_SELECT_LIMIT
        );
        collectNode.keepContextForFetcher(true);

        FetchProjection fetchProjection = getFetchProjection(characters, collectSymbols, outputSymbols, collectNode, ctx);

        MergeNode localMergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(fetchProjection),
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().size(), is(1));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, "Ford")));
    }

    private FetchProjection getFetchProjection(DocTableInfo characters, List<Symbol> collectSymbols, List<Symbol> outputSymbols, CollectNode collectNode, Planner.Context ctx) {
        Map<Integer, List<String>> executionNodes = new HashMap<>();
        executionNodes.put(collectNode.executionNodeId(), new ArrayList<>(collectNode.executionNodes()));
        return new FetchProjection(
                ctx.jobSearchContextIdToExecutionNodeId(),
                new InputColumn(0, DataTypes.STRING), collectSymbols, outputSymbols,
                characters.partitionedByColumns(),
                executionNodes,
                5,
                false,
                ctx.jobSearchContextIdToNode(),
                ctx.jobSearchContextIdToShard()
        );
    }

    @Test
    public void testQTFTaskOrdered() throws Exception {
        // select id, name from characters order by name, female;
        setup.setUpCharacters();
        DocTableInfo characters = docSchemaInfo.getTableInfo("characters");

        OrderBy orderBy = new OrderBy(Arrays.<Symbol>asList(nameRef, femaleRef),
                new boolean[]{false, false},
                new Boolean[]{false, false});

        ReferenceInfo docIdRefInfo = characters.getReferenceInfo(new ColumnIdent(DocSysColumns.DOCID.name()));
        // add nameRef and femaleRef to collectSymbols because this are ordered by values
        List<Symbol> collectSymbols = Lists.<Symbol>newArrayList(new Reference(docIdRefInfo), nameRef, femaleRef);
        List<Symbol> outputSymbols = Lists.<Symbol>newArrayList(idRef, nameRef);

        MergeProjection mergeProjection = new MergeProjection(
                collectSymbols,
                orderBy.orderBySymbols(),
                orderBy.reverseFlags(),
                orderBy.nullsFirst()
        );
        Planner.Context ctx = new Planner.Context(clusterService());

        CollectNode collectNode = PlanNodeBuilder.collect(
                characters,
                ctx,
                WhereClause.MATCH_ALL,
                collectSymbols,
                ImmutableList.<Projection>of(),
                orderBy,
                Constants.DEFAULT_SELECT_LIMIT
        );
        collectNode.projections(ImmutableList.<Projection>of(mergeProjection));
        collectNode.keepContextForFetcher(true);

        FetchProjection fetchProjection = getFetchProjection(characters, collectSymbols, outputSymbols, collectNode, ctx);

        MergeNode localMergeNode = PlanNodeBuilder.sortedLocalMerge(
                ImmutableList.<Projection>of(fetchProjection),
                orderBy,
                collectSymbols,
                null,
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().size(), is(1));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(
                isRow(1, "Arthur"),
                isRow(4, "Arthur"),
                isRow(2, "Ford"),
                isRow(3, "Trillian")
        ));
    }

    @Test
    public void testQTFTaskWithFunction() throws Exception {
        // select id, date_trunc('day', date) from searchf where id = 2;
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
        ), Arrays.asList(Literal.newLiteral("month"), date_ref));
        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.INTEGER, DataTypes.INTEGER)),
                DataTypes.BOOLEAN),
                Arrays.asList(id_ref, Literal.newLiteral(2))
        );

        DocTableInfo searchf = docSchemaInfo.getTableInfo("searchf");
        ReferenceInfo docIdRefInfo = searchf.getReferenceInfo(new ColumnIdent(DocSysColumns.DOCID.name()));

        Planner.Context ctx = new Planner.Context(clusterService());
        List<Symbol> collectSymbols = ImmutableList.<Symbol>of(new Reference(docIdRefInfo));
        CollectNode collectNode = PlanNodeBuilder.collect(
                searchf,
                ctx,
                new WhereClause(whereClause),
                collectSymbols,
                ImmutableList.<Projection>of(),
                null,
                Constants.DEFAULT_SELECT_LIMIT
        );
        collectNode.keepContextForFetcher(true);

        TopNProjection topN = new TopNProjection(2, TopN.NO_OFFSET);
        topN.outputs(Collections.<Symbol>singletonList(new InputColumn(0)));

        FetchProjection fetchProjection = getFetchProjection(searchf, collectSymbols, Arrays.asList(id_ref, function), collectNode, ctx);

        MergeNode mergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.of(topN, fetchProjection),
                collectNode,
                ctx);
        Plan plan = new QueryThenFetch(collectNode, mergeNode);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().size(), is(1));

        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, 315532800000L)));
    }

    @Test
    public void testQTFTaskPartitioned() throws Exception {
        setup.setUpPartitionedTableWithName();
        DocTableInfo parted = docSchemaInfo.getTableInfo("parted");
        Planner.Context ctx = new Planner.Context(clusterService());

        ReferenceInfo docIdRefInfo = parted.getReferenceInfo(new ColumnIdent(DocSysColumns.DOCID.name()));
        List<Symbol> collectSymbols = Lists.<Symbol>newArrayList(new Reference(docIdRefInfo));
        List<Symbol> outputSymbols =  Arrays.<Symbol>asList(partedIdRef, partedNameRef, partedDateRef);

        CollectNode collectNode = PlanNodeBuilder.collect(
                parted,
                ctx,
                WhereClause.MATCH_ALL,
                collectSymbols,
                ImmutableList.<Projection>of(),
                null,
                Constants.DEFAULT_SELECT_LIMIT
        );
        collectNode.keepContextForFetcher(true);

        FetchProjection fetchProjection = getFetchProjection(parted, collectSymbols, outputSymbols, collectNode, ctx);

        MergeNode localMergeNode = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(fetchProjection),
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode);
        Job job = executor.newJob(plan);

        assertThat(job.tasks().size(), is(1));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, containsInAnyOrder(
                isRow(3, "Ford", 1396388720242L),
                isRow(1, "Trillian", null),
                isRow(2, null, 0L)
        ));
    }

    @Test
    public void testESDeleteByQueryTask() throws Exception {
        setup.setUpCharacters();

        Function whereClause = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.STRING, DataTypes.STRING)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(idRef, Literal.newLiteral(2)));

        ESDeleteByQueryNode node = new ESDeleteByQueryNode(
                1,
                ImmutableList.of(new String[]{"characters"}),
                ImmutableList.of(new WhereClause(whereClause)));
        Plan plan = new IterablePlan(node);
        Job job = executor.newJob(plan);
        ESDeleteByQueryTask task = (ESDeleteByQueryTask) job.tasks().get(0);

        task.start();
        TaskResult taskResult = task.result().get(0).get();
        Bucket rows = taskResult.rows();
        assertThat(rows, contains(isRow(-1L)));

        // verify deletion
        execute("select * from characters where id = 2");
        assertThat(response.rowCount(), is(0L));
    }

    @Test
    public void testInsertWithSymbolBasedUpsertByIdTask() throws Exception {
        execute("create table characters (id int primary key, name string)");
        ensureGreen();

        /* insert into characters (id, name) values (99, 'Marvin'); */
        Planner.Context ctx = new Planner.Context(clusterService());
        SymbolBasedUpsertByIdNode updateNode = new SymbolBasedUpsertByIdNode(
                ctx.nextExecutionNodeId(),
                false,
                false,
                null,
                new Reference[]{idRef, nameRef});
        updateNode.add("characters", "99", "99", null, null, new Object[]{99, new BytesRef("Marvin")});

        Plan plan = new IterablePlan(updateNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(SymbolBasedUpsertByIdTask.class));

        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Bucket rows = taskResult.rows();
        assertThat(rows, contains(isRow(1L)));

        // verify insertion
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode getNode = newGetNode("characters", outputs, "99", ctx.nextExecutionNodeId());
        plan = new IterablePlan(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Bucket objects = result.get(0).get().rows();

        assertThat(objects, contains(isRow(99, "Marvin")));
    }

    @Test
    public void testInsertIntoPartitionedTableWithSymbolBasedUpsertByIdTask() throws Exception {
        execute("create table parted (" +
                "  id int, " +
                "  name string, " +
                "  date timestamp" +
                ") partitioned by (date)");
        ensureGreen();

        /* insert into parted (id, name, date) values(0, 'Trillian', 13959981214861); */
        Planner.Context ctx = new Planner.Context(clusterService());
        SymbolBasedUpsertByIdNode updateNode = new SymbolBasedUpsertByIdNode(
                ctx.nextExecutionNodeId(),
                true,
                false,
                null,
                new Reference[]{idRef, nameRef});

        PartitionName partitionName = new PartitionName("parted", Arrays.asList(new BytesRef("13959981214861")));
        updateNode.add(partitionName.stringValue(), "123", "123", null, null, new Object[]{0L, new BytesRef("Trillian")});

        Plan plan = new IterablePlan(updateNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(SymbolBasedUpsertByIdTask.class));

        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Bucket indexResult = taskResult.rows();
        assertThat(indexResult, contains(isRow(1L)));

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
        assertThat((String) hits.getHits()[0].field("name").getValues().get(0), is("Trillian"));
    }

    @Test
    public void testInsertMultiValuesWithSymbolBasedUpsertByIdTask() throws Exception {
        execute("create table characters (id int primary key, name string)");
        ensureGreen();

        /* insert into characters (id, name) values (99, 'Marvin'), (42, 'Deep Thought'); */
        Planner.Context ctx = new Planner.Context(clusterService());
        SymbolBasedUpsertByIdNode updateNode = new SymbolBasedUpsertByIdNode(
                ctx.nextExecutionNodeId(),
                false,
                false,
                null,
                new Reference[]{idRef, nameRef});

        updateNode.add("characters", "99", "99", null, null, new Object[]{99, new BytesRef("Marvin")});
        updateNode.add("characters", "42", "42", null, null, new Object[]{42, new BytesRef("Deep Thought")});

        Plan plan = new IterablePlan(updateNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(SymbolBasedUpsertByIdTask.class));

        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Bucket rows = taskResult.rows();
        assertThat(rows, contains(isRow(2L)));

        // verify insertion
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode getNode = newGetNode("characters", outputs, Arrays.asList("99", "42"), ctx.nextExecutionNodeId());
        plan = new IterablePlan(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Bucket objects = result.get(0).get().rows();

        assertThat(objects, contains(
                isRow(99, "Marvin"),
                isRow(42, "Deep Thought")
        ));
    }

    @Test
    public void testUpdateWithSymbolBasedUpsertByIdTask() throws Exception {
        setup.setUpCharacters();

        // update characters set name='Vogon lyric fan' where id=1
        Planner.Context ctx = new Planner.Context(clusterService());
        SymbolBasedUpsertByIdNode updateNode = new SymbolBasedUpsertByIdNode(
                ctx.nextExecutionNodeId(), false, false, new String[]{nameRef.ident().columnIdent().fqn()}, null);
        updateNode.add("characters", "1", "1", new Symbol[]{Literal.newLiteral("Vogon lyric fan")}, null);
        Plan plan = new IterablePlan(updateNode);

        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(SymbolBasedUpsertByIdTask.class));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Bucket rows = taskResult.rows();
        assertThat(rows, contains(isRow(1L)));

        // verify update
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        ESGetNode getNode = newGetNode("characters", outputs, "1", ctx.nextExecutionNodeId());
        plan = new IterablePlan(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Bucket objects = result.get(0).get().rows();

        assertThat(objects, contains(isRow(1, "Vogon lyric fan")));
    }

    @Test
    public void testInsertOnDuplicateWithSymbolBasedUpsertByIdTask() throws Exception {
        setup.setUpCharacters();
        /* insert into characters (id, name, female) values (5, 'Zaphod Beeblebrox', false)
           on duplicate key update set name = 'Zaphod Beeblebrox'; */
        Object[] missingAssignments = new Object[]{5, new BytesRef("Zaphod Beeblebrox"), false};
        Planner.Context ctx = new Planner.Context(clusterService());
        SymbolBasedUpsertByIdNode updateNode = new SymbolBasedUpsertByIdNode(
                ctx.nextExecutionNodeId(),
                false,
                false,
                new String[]{nameRef.ident().columnIdent().fqn()},
                new Reference[]{idRef, nameRef, femaleRef});

        updateNode.add("characters", "5", "5", new Symbol[]{Literal.newLiteral("Zaphod Beeblebrox")}, null, missingAssignments);
        Plan plan = new IterablePlan(updateNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(SymbolBasedUpsertByIdTask.class));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Bucket rows = taskResult.rows();
        assertThat(rows, contains(isRow(1L)));

        // verify insert
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef, femaleRef);
        ESGetNode getNode = newGetNode("characters", outputs, "5", ctx.nextExecutionNodeId());
        plan = new IterablePlan(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Bucket objects = result.get(0).get().rows();
        assertThat(objects, contains(isRow(5, "Zaphod Beeblebrox", false)));

    }

    @Test
    public void testUpdateOnDuplicateWithSymbolBasedUpsertByIdTask() throws Exception {
        setup.setUpCharacters();
        /* insert into characters (id, name, female) values (1, 'Zaphod Beeblebrox', false)
           on duplicate key update set name = 'Zaphod Beeblebrox'; */
        Object[] missingAssignments = new Object[]{1, new BytesRef("Zaphod Beeblebrox"), true};
        Planner.Context ctx = new Planner.Context(clusterService());
        SymbolBasedUpsertByIdNode updateNode = new SymbolBasedUpsertByIdNode(
                ctx.nextExecutionNodeId(),
                false,
                false,
                new String[]{femaleRef.ident().columnIdent().fqn()},
                new Reference[]{idRef, nameRef, femaleRef});
        updateNode.add("characters", "1", "1", new Symbol[]{Literal.newLiteral(true)}, null, missingAssignments);
        Plan plan = new IterablePlan(updateNode);
        Job job = executor.newJob(plan);
        assertThat(job.tasks().get(0), instanceOf(SymbolBasedUpsertByIdTask.class));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        TaskResult taskResult = result.get(0).get();
        Bucket rows = taskResult.rows();
        assertThat(rows, contains(isRow(1L)));

        // verify update
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef, femaleRef);
        ESGetNode getNode = newGetNode("characters", outputs, "1", ctx.nextExecutionNodeId());
        plan = new IterablePlan(getNode);
        job = executor.newJob(plan);
        result = executor.execute(job);
        Bucket objects = result.get(0).get().rows();

        assertThat(objects, contains(isRow(1, "Arthur", true)));
    }

    @Test
    public void testBulkUpdateByQueryTask() throws Exception {
        setup.setUpCharacters();
        /* update characters set name 'Zaphod Beeblebrox' where female = false
           update characters set name 'Zaphod Beeblebrox' where female = true
         */

        List<Plan> childNodes = new ArrayList<>();
        Planner.Context plannerContext = new Planner.Context(clusterService());

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        Reference uidReference = new Reference(
                new ReferenceInfo(
                        new ReferenceIdent(tableInfo.ident(), "_uid"),
                        RowGranularity.DOC, DataTypes.STRING));

        // 1st collect and merge nodes
        Function whereClause1 = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.BOOLEAN, DataTypes.BOOLEAN)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(femaleRef, Literal.newLiteral(true)));

        UpdateProjection updateProjection = new UpdateProjection(
                new InputColumn(0, DataTypes.STRING),
                new String[]{"name"},
                new Symbol[]{Literal.newLiteral("Zaphod Beeblebrox")},
                null);

        CollectNode collectNode1 = PlanNodeBuilder.collect(
                tableInfo,
                plannerContext,
                new WhereClause(whereClause1),
                ImmutableList.<Symbol>of(uidReference),
                ImmutableList.<Projection>of(updateProjection),
                null,
                Preference.PRIMARY.type()
        );
        MergeNode mergeNode1 = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION), collectNode1,
                plannerContext);
        childNodes.add(new CollectAndMerge(collectNode1, mergeNode1));

        // 2nd collect and merge nodes
        Function whereClause2 = new Function(new FunctionInfo(
                new FunctionIdent(EqOperator.NAME, Arrays.<DataType>asList(DataTypes.BOOLEAN, DataTypes.BOOLEAN)),
                DataTypes.BOOLEAN),
                Arrays.<Symbol>asList(femaleRef, Literal.newLiteral(true)));

        CollectNode collectNode2 = PlanNodeBuilder.collect(
                tableInfo,
                plannerContext,
                new WhereClause(whereClause2),
                ImmutableList.<Symbol>of(uidReference),
                ImmutableList.<Projection>of(updateProjection),
                null,
                Preference.PRIMARY.type()
        );
        MergeNode mergeNode2 = PlanNodeBuilder.localMerge(
                ImmutableList.<Projection>of(CountAggregation.PARTIAL_COUNT_AGGREGATION_PROJECTION), collectNode2,
                plannerContext);
        childNodes.add(new CollectAndMerge(collectNode2, mergeNode2));

        Upsert plan = new Upsert(childNodes);
        Job job = executor.newJob(plan);

        assertThat(job.tasks().size(), is(1));
        assertThat(job.tasks().get(0), instanceOf(ExecutionNodesTask.class));
        List<? extends ListenableFuture<TaskResult>> results = executor.execute(job);
        assertThat(results.size(), is(2));

        for (int i = 0; i < results.size(); i++) {
            TaskResult result = results.get(i).get();
            assertThat(result, instanceOf(RowCountResult.class));
            // each of the bulk request hits 2 records
            assertThat(((RowCountResult)result).rowCount(), is(2L));
        }
    }

    @Test
    public void testKillTask() throws Exception {
        Job job = executor.newJob(KillPlan.INSTANCE);
        assertThat(job.tasks(), hasSize(1));
        assertThat(job.tasks().get(0), instanceOf(KillTask.class));

        List<? extends ListenableFuture<TaskResult>> results = executor.execute(job);
        assertThat(results, hasSize(1));
        results.get(0).get();
    }
}
