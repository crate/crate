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
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.*;
import io.crate.core.collections.Bucket;
import io.crate.executor.Job;
import io.crate.executor.Task;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.kill.KillableCallable;
import io.crate.executor.transport.task.KillTask;
import io.crate.executor.transport.task.elasticsearch.ESDeleteByQueryTask;
import io.crate.executor.transport.task.elasticsearch.ESGetTask;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.operation.operator.EqOperator;
import io.crate.operation.projectors.TopN;
import io.crate.operation.scalar.DateTruncFunction;
import io.crate.planner.*;
import io.crate.planner.node.dml.ESDeleteByQueryNode;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.node.dql.ESGetNode;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.node.management.KillPlan;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.MergeProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.TopNProjection;
import io.crate.planner.symbol.*;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.lang.reflect.Field;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Map;

import static io.crate.testing.TestingHelpers.isRow;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

public class TransportExecutorTest extends BaseTransportExecutorTest {

    @Test
    public void testESGetTask() throws Exception {
        setup.setUpCharacters();

        // create plan
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());
        ESGetNode node = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());
        Plan plan = new IterablePlan(ctx.jobId(), node);
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
        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());
        ESGetNode node = newGetNode("characters", outputs, "2", ctx.nextExecutionPhaseId());
        Plan plan = new IterablePlan(ctx.jobId(), node);
        Job job = executor.newJob(plan);
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, null)));
    }

    @Test
    public void testESMultiGet() throws Exception {
        setup.setUpCharacters();
        ImmutableList<Symbol> outputs = ImmutableList.<Symbol>of(idRef, nameRef);
        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());
        ESGetNode node = newGetNode("characters", outputs, asList("1", "2"), ctx.nextExecutionPhaseId());
        Plan plan = new IterablePlan(ctx.jobId(), node);
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

        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());

        CollectPhase collectNode = PlanNodeBuilder.collect(
                ctx.jobId(),
                characters,
                ctx,
                WhereClause.MATCH_ALL,
                collectSymbols,
                ImmutableList.<Projection>of(),
                null,
                Constants.DEFAULT_SELECT_LIMIT
        );
        collectNode.keepContextForFetcher(true);

        FetchProjection fetchProjection = getFetchProjection((DocTableInfo) characters, (List<Symbol>) collectSymbols, (List<Symbol>) outputSymbols, (CollectPhase) collectNode, ctx);

        MergePhase localMergeNode = PlanNodeBuilder.localMerge(
                ctx.jobId(),
                ImmutableList.<Projection>of(fetchProjection),
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode, ctx.jobId());

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

        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());
        CollectPhase collectNode = PlanNodeBuilder.collect(
                ctx.jobId(),
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

        MergePhase localMergeNode = PlanNodeBuilder.localMerge(
                ctx.jobId(),
                ImmutableList.<Projection>of(fetchProjection),
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode, ctx.jobId());

        Job job = executor.newJob(plan);
        assertThat(job.tasks().size(), is(1));
        List<? extends ListenableFuture<TaskResult>> result = executor.execute(job);
        Bucket rows = result.get(0).get().rows();
        assertThat(rows, contains(isRow(2, "Ford")));
    }

    private FetchProjection getFetchProjection(DocTableInfo characters, List<Symbol> collectSymbols, List<Symbol> outputSymbols, CollectPhase collectNode, Planner.Context ctx) {
        return new FetchProjection(
                collectNode.executionPhaseId(),
                new InputColumn(0, DataTypes.STRING), collectSymbols, outputSymbols,
                characters.partitionedByColumns(),
                collectNode.executionNodes(),
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
        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());

        CollectPhase collectNode = PlanNodeBuilder.collect(
                ctx.jobId(),
                characters,
                ctx,
                WhereClause.MATCH_ALL,
                collectSymbols,
                ImmutableList.<Projection>of(mergeProjection),
                orderBy,
                Constants.DEFAULT_SELECT_LIMIT
        );
        collectNode.keepContextForFetcher(true);

        FetchProjection fetchProjection = getFetchProjection(characters, collectSymbols, outputSymbols, collectNode, ctx);

        MergePhase localMergeNode = PlanNodeBuilder.sortedLocalMerge(
                ctx.jobId(),
                ImmutableList.<Projection>of(fetchProjection),
                orderBy,
                collectSymbols,
                null,
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode, ctx.jobId());

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
                        new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "searchf"),
                        "id"),
                RowGranularity.DOC,
                DataTypes.INTEGER
        ));
        Reference date_ref = new Reference(new ReferenceInfo(
                new ReferenceIdent(
                        new TableIdent(Schemas.DEFAULT_SCHEMA_NAME, "searchf"),
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

        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());
        List<Symbol> collectSymbols = ImmutableList.<Symbol>of(new Reference(docIdRefInfo));
        UUID jobId = UUID.randomUUID();
        CollectPhase collectNode = PlanNodeBuilder.collect(
                ctx.jobId(),
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

        MergePhase mergeNode = PlanNodeBuilder.localMerge(
                jobId,
                ImmutableList.of(topN, fetchProjection),
                collectNode,
                ctx);
        Plan plan = new QueryThenFetch(collectNode, mergeNode, jobId);

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
        Planner.Context ctx = new Planner.Context(clusterService(), UUID.randomUUID());

        ReferenceInfo docIdRefInfo = parted.getReferenceInfo(new ColumnIdent(DocSysColumns.DOCID.name()));
        List<Symbol> collectSymbols = Lists.<Symbol>newArrayList(new Reference(docIdRefInfo));
        List<Symbol> outputSymbols =  Arrays.<Symbol>asList(partedIdRef, partedNameRef, partedDateRef);

        UUID jobId = UUID.randomUUID();
        CollectPhase collectNode = PlanNodeBuilder.collect(
                ctx.jobId(),
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

        MergePhase localMergeNode = PlanNodeBuilder.localMerge(
                jobId,
                ImmutableList.<Projection>of(fetchProjection),
                collectNode,
                ctx);

        Plan plan = new QueryThenFetch(collectNode, localMergeNode, jobId);
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
        Plan plan = new IterablePlan(UUID.randomUUID(), node);
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
    public void testKillTask() throws Exception {
        Job job = executor.newJob(new KillPlan(UUID.randomUUID()));
        assertThat(job.tasks(), hasSize(1));
        assertThat(job.tasks().get(0), instanceOf(KillTask.class));

        List<? extends ListenableFuture<TaskResult>> results = executor.execute(job);
        assertThat(results, hasSize(1));
        results.get(0).get();
    }

    @Test
    public void testKillJobTask() throws Exception {
        execute("create table t (name string)");
        refresh();
        Statement statement = SqlParser.createStatement("select * from t");
        Analyzer analyzer = internalCluster().getInstance(Analyzer.class);
        Planner planner = internalCluster().getInstance(Planner.class);

        UUID jobId = UUID.randomUUID();

        Analysis analysis = analyzer.analyze(statement, new ParameterContext(new Object[0], new Object[0][], null));
        QueryThenFetch plan = ((QueryThenFetch) planner.plan(analysis, jobId));

        MergePhase mergePhase = plan.mergeNode();

        MergePhase fishyMergePhase = new MergePhase(jobId,
                mergePhase.executionPhaseId(),
                mergePhase.name(),
                mergePhase.numUpstreams() + 1,
                mergePhase.inputTypes(),
                mergePhase.projections()
        );
        QueryThenFetch brokenPlan = new QueryThenFetch(plan.collectNode(), fishyMergePhase, jobId);

        TransportExecutor transportExecutor = internalCluster().getInstance(TransportExecutor.class);
        Job job = transportExecutor.newJob(brokenPlan);
        transportExecutor.execute(job);

        execute(String.format("KILL '%s'", jobId));

        final Field activeContexts = JobContextService.class.getDeclaredField("activeContexts");
        final Field activeOperations = TransportShardUpsertAction.class.getDeclaredField("activeOperations");
        final Field activeOperationsSb = SymbolBasedTransportShardUpsertAction.class.getDeclaredField("activeOperations");

        activeContexts.setAccessible(true);
        activeOperations.setAccessible(true);
        activeOperationsSb.setAccessible(true);

        for (JobContextService jobContextService : internalCluster().getInstances(JobContextService.class)) {
            Map<UUID, JobExecutionContext> contexts = (Map<UUID, JobExecutionContext>) activeContexts.get(jobContextService);
            assertThat(contexts.containsKey(jobId), not(true));
        }
        for (TransportShardUpsertAction action : internalCluster().getInstances(TransportShardUpsertAction.class)) {
            Multimap<UUID, KillableCallable> operations = (Multimap<UUID, KillableCallable>) activeOperations.get(action);
            assertThat(operations.containsKey(jobId), not(true));
        }
        for (SymbolBasedTransportShardUpsertAction action : internalCluster().getInstances(SymbolBasedTransportShardUpsertAction.class)) {
            Multimap<UUID, KillableCallable> operations = (Multimap<UUID, KillableCallable>) activeOperationsSb.get(action);
            assertThat(operations.containsKey(jobId), not(true));
        }
    }
}
