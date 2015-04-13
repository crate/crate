/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.integrationtests;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.Constants;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Row;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.NodeFetchRequest;
import io.crate.executor.transport.NodeFetchResponse;
import io.crate.executor.transport.TransportExecutor;
import io.crate.executor.transport.TransportFetchNodeAction;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.scalar.SubstrFunction;
import io.crate.planner.*;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.MergeProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.projection.builder.ProjectionBuilder;
import io.crate.planner.symbol.*;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.test.integration.CrateTestCluster;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

    Setup setup = new Setup(sqlExecutor);
    TransportExecutor executor;
    DocSchemaInfo docSchemaInfo;

    @Before
    public void transportSetUp() {
        CrateTestCluster cluster = cluster();
        executor = cluster.getInstance(TransportExecutor.class);
        docSchemaInfo = cluster.getInstance(DocSchemaInfo.class);
    }

    @After
    public void transportTearDown() {
        executor = null;
        docSchemaInfo = null;
    }

    private void setUpCharacters() {
        sqlExecutor.exec("create table characters (id int primary key, name string) " +
                "clustered into 2 shards with(number_of_replicas=0)");
        sqlExecutor.ensureGreen();
        sqlExecutor.exec("insert into characters (id, name) values (?, ?)",
                new Object[][]{
                        new Object[] { 1, "Arthur"},
                        new Object[] { 2, "Ford"},
                }
        );
        sqlExecutor.refresh("characters");
    }

    private Job createCollectJob(Planner.Context plannerContext, boolean keepContextForFetcher) {
        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");

        ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));
        Symbol docIdRef = new Reference(docIdRefInfo);
        List<Symbol> toCollect = ImmutableList.of(docIdRef);

        List<DataType> outputTypes = new ArrayList<>(toCollect.size());
        for (Symbol symbol : toCollect) {
            outputTypes.add(symbol.valueType());
        }

        CollectNode collectNode = new CollectNode(
                plannerContext.nextExecutionNodeId(),
                "collect",
                tableInfo.getRouting(WhereClause.MATCH_ALL, null));
        collectNode.toCollect(toCollect);
        collectNode.outputTypes(outputTypes);
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.keepContextForFetcher(keepContextForFetcher);
        collectNode.jobId(UUID.randomUUID());
        plannerContext.allocateJobSearchContextIds(collectNode.routing());

        Plan plan = new IterablePlan(collectNode);
        return executor.newJob(plan);
    }

    @Test
    public void testCollectDocId() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = new Planner.Context(clusterService());
        Job job = createCollectJob(plannerContext, false);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);

        assertThat(result.size(), is(2));
        int seenJobSearchContextId = -1;
        for (ListenableFuture<TaskResult> nodeResult : result) {
            TaskResult taskResult = nodeResult.get();
            assertThat(taskResult.rows().size(), is(1));
            Object docIdCol = taskResult.rows().iterator().next().get(0);
            assertNotNull(docIdCol);
            assertThat(docIdCol, instanceOf(Long.class));
            long docId = (long)docIdCol;
            // unpack jobSearchContextId and reader doc id from docId
            int jobSearchContextId = (int)(docId >> 32);
            int doc = (int)docId;
            assertThat(doc, is(0));
            assertThat(jobSearchContextId, greaterThan(-1));
            if (seenJobSearchContextId == -1) {
                assertThat(jobSearchContextId, anyOf(is(0), is(1)));
                seenJobSearchContextId = jobSearchContextId;
            } else {
                assertThat(jobSearchContextId, is(seenJobSearchContextId == 0 ? 1 : 0));
            }
        }

    }

    @Test
    public void testFetchAction() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = new Planner.Context(clusterService());
        Job job = createCollectJob(plannerContext, true);
        List<ListenableFuture<TaskResult>> result = executor.execute(job);
        TransportFetchNodeAction transportFetchNodeAction = cluster().getInstance(TransportFetchNodeAction.class);

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo idRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), DocSysColumns.DOC.name(), ImmutableList.of("id")),
                RowGranularity.DOC,
                IntegerType.INSTANCE
        );
        ReferenceInfo nameRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), DocSysColumns.DOC.name(), ImmutableList.of("name")),
                RowGranularity.DOC,
                StringType.INSTANCE
        );
        List<Reference> toFetchReferences = ImmutableList.of(new Reference(idRefInfo), new Reference(nameRefInfo));

        // extract docIds by nodeId and jobSearchContextId
        Map<String, LongArrayList> jobSearchContextDocIds = new HashMap<>();
        for (ListenableFuture<TaskResult> nodeResult : result) {
            TaskResult taskResult = nodeResult.get();
            long docId = (long)taskResult.rows().iterator().next().get(0);
            // unpack jobSearchContextId and reader doc id from docId
            int jobSearchContextId = (int)(docId >> 32);
            String nodeId = plannerContext.nodeId(jobSearchContextId);
            LongArrayList docIdsPerNode = jobSearchContextDocIds.get(nodeId);
            if (docIdsPerNode == null) {
                docIdsPerNode = new LongArrayList();
                jobSearchContextDocIds.put(nodeId, docIdsPerNode);
            }
            docIdsPerNode.add(docId);
        }

        final CountDownLatch latch = new CountDownLatch(jobSearchContextDocIds.size());
        final List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, LongArrayList> nodeEntry : jobSearchContextDocIds.entrySet()) {
            NodeFetchRequest nodeFetchRequest = new NodeFetchRequest();
            nodeFetchRequest.jobId(job.id());
            nodeFetchRequest.toFetchReferences(toFetchReferences);
            nodeFetchRequest.closeContext(true);
            nodeFetchRequest.jobSearchContextDocIds(nodeEntry.getValue());

            transportFetchNodeAction.execute(nodeEntry.getKey(), nodeFetchRequest, new ActionListener<NodeFetchResponse>() {
                @Override
                public void onResponse(NodeFetchResponse nodeFetchResponse) {
                    for (Row row : nodeFetchResponse.rows()) {
                        rows.add(row);
                    }
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    latch.countDown();
                    fail(e.getMessage());
                }
            });
        }
        latch.await();

        assertThat(rows.size(), is(2));
        Iterator<Row> it = rows.iterator();
        while (it.hasNext()) {
            Row row = it.next();
            assertThat((Integer) row.get(0), anyOf(is(1), is(2)));
            assertThat((BytesRef) row.get(1), anyOf(is(new BytesRef("Arthur")), is(new BytesRef("Ford"))));
        }
    }

    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = new Planner.Context(clusterService());

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo idRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), "id"),
                RowGranularity.DOC,
                IntegerType.INSTANCE
        );
        ReferenceInfo nameRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), "name"),
                RowGranularity.DOC,
                StringType.INSTANCE
        );
        ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));

        Symbol docIdSymbol = new InputColumn(0, DataTypes.STRING);
        List<Symbol> inputSymbols = ImmutableList.<Symbol>of(new Reference(docIdRefInfo), new Reference(idRefInfo));
        final List<Symbol> outputSymbols = ImmutableList.of(
                new Reference(idRefInfo),
                new Reference(nameRefInfo),
                new Function(
                        new FunctionInfo(
                                new FunctionIdent(SubstrFunction.NAME, ImmutableList.<DataType>of(StringType.INSTANCE, IntegerType.INSTANCE)),
                                StringType.INSTANCE),
                        Arrays.asList(new Reference(nameRefInfo), Literal.newLiteral(2))
                )
        );

        QuerySpec querySpec = new QuerySpec();
        querySpec.where(WhereClause.MATCH_ALL);
        querySpec.orderBy(new OrderBy(
                ImmutableList.<Symbol>of(new Reference(idRefInfo)),
                new boolean[]{false}, new Boolean[]{false}));

        CollectNode collectNode = new CollectNode(
                plannerContext.nextExecutionNodeId(),
                "collect",
                tableInfo.getRouting(WhereClause.MATCH_ALL, null));
        collectNode.toCollect(inputSymbols);
        collectNode.jobId(UUID.randomUUID());
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.keepContextForFetcher(true);
        List<DataType> outputTypes = new ArrayList<>(inputSymbols.size());
        for (Symbol symbol : inputSymbols) {
            outputTypes.add(symbol.valueType());
        }
        collectNode.outputTypes(outputTypes);
        collectNode.orderBy(querySpec.orderBy());
        plannerContext.allocateJobSearchContextIds(collectNode.routing());

        ProjectionBuilder projectionBuilder = new ProjectionBuilder(querySpec);
        MergeProjection mergeProjection = projectionBuilder.mergeProjection(
                inputSymbols,
                querySpec.orderBy());
        collectNode.projections(ImmutableList.<Projection>of(mergeProjection));

        FetchProjection fetchProjection = new FetchProjection(
                docIdSymbol, inputSymbols, outputSymbols, ImmutableList.<ReferenceInfo>of(),
                collectNode.executionNodes());

        MergeNode mergeNode = MergeNode.sortedMergeNode(
                plannerContext.nextExecutionNodeId(),
                "sorted merge", collectNode.executionNodes().size(),
                new int[]{1}, new boolean[]{false}, new Boolean[]{false});
        mergeNode.jobSearchContextIdToNode(plannerContext.jobSearchContextIdToNode());
        mergeNode.jobSearchContextIdToShard(plannerContext.jobSearchContextIdToShard());
        mergeNode.projections(ImmutableList.<Projection>of(fetchProjection));

        Plan plan = new IterablePlan(collectNode, mergeNode);
        Job job = executor.newJob(plan);

        final List<Object[]> resultingRows = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ListenableFuture<List<TaskResult>> results = Futures.allAsList(executor.execute(job));
        Futures.addCallback(results, new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(List<TaskResult> resultList) {
                for (Row row : resultList.get(0).rows()) {
                    resultingRows.add(row.materialize());
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
                fail(t.getMessage());
            }
        });

        latch.await();
        assertThat(resultingRows.size(), is(2));
        assertThat(resultingRows.get(0).length, is(outputSymbols.size()));
        assertThat((Integer) resultingRows.get(0)[0], is(1));
        assertThat((BytesRef) resultingRows.get(0)[1], is(new BytesRef("Arthur")));
        assertThat((BytesRef) resultingRows.get(0)[2], is(new BytesRef("rthur")));
        assertThat((Integer) resultingRows.get(1)[0], is(2));
        assertThat((BytesRef) resultingRows.get(1)[1], is(new BytesRef("Ford")));
        assertThat((BytesRef) resultingRows.get(1)[2], is(new BytesRef("ord")));
    }

    @Test
    public void testFetchProjectionWithBulkSize() throws Exception {
        /**
         * Setup scenario where more docs per node exists than the configured bulkSize,
         * so multiple request to one node must be done and merged together.
         */
        setup.setUpLocations();
        sqlExecutor.refresh("locations");
        int bulkSize = 2;
        Planner.Context plannerContext = new Planner.Context(clusterService());

        TableInfo tableInfo = docSchemaInfo.getTableInfo("locations");
        ReferenceInfo positionRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), "position"),
                RowGranularity.DOC,
                IntegerType.INSTANCE
        );
        ReferenceInfo nameRefInfo = new ReferenceInfo(
                new ReferenceIdent(tableInfo.ident(), "name"),
                RowGranularity.DOC,
                StringType.INSTANCE
        );
        ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));

        Symbol docIdSymbol = new InputColumn(0, DataTypes.STRING);
        List<Symbol> inputSymbols = ImmutableList.<Symbol>of(new Reference(docIdRefInfo), new Reference(positionRefInfo));
        final List<Symbol> outputSymbols = ImmutableList.<Symbol>of(new Reference(positionRefInfo), new Reference(nameRefInfo));

        QuerySpec querySpec = new QuerySpec();
        querySpec.where(WhereClause.MATCH_ALL);
        querySpec.orderBy(new OrderBy(
                ImmutableList.<Symbol>of(new Reference(positionRefInfo)),
                new boolean[]{false}, new Boolean[]{false}));

        ProjectionBuilder projectionBuilder = new ProjectionBuilder(querySpec);
        MergeProjection mergeProjection = projectionBuilder.mergeProjection(
                inputSymbols,
                querySpec.orderBy());

        CollectNode collectNode = PlanNodeBuilder.collect(
                tableInfo,
                plannerContext,
                querySpec.where(),
                inputSymbols,
                ImmutableList.<Projection>of(mergeProjection),
                querySpec.orderBy(),
                MoreObjects.firstNonNull(querySpec.limit(), Constants.DEFAULT_SELECT_LIMIT) + querySpec.offset()
        );
        collectNode.keepContextForFetcher(true);

        FetchProjection fetchProjection = new FetchProjection(
                docIdSymbol, inputSymbols, outputSymbols, ImmutableList.<ReferenceInfo>of(),
                collectNode.executionNodes(), bulkSize, querySpec.isLimited());

        MergeNode mergeNode = MergeNode.sortedMergeNode(
                plannerContext.nextExecutionNodeId(),
                "sorted merge", collectNode.executionNodes().size(),
                new int[]{1}, new boolean[]{false}, new Boolean[]{false});
        mergeNode.jobSearchContextIdToNode(plannerContext.jobSearchContextIdToNode());
        mergeNode.jobSearchContextIdToShard(plannerContext.jobSearchContextIdToShard());
        mergeNode.projections(ImmutableList.<Projection>of(fetchProjection));

        Plan plan = new IterablePlan(collectNode, mergeNode);
        Job job = executor.newJob(plan);

        final List<Object[]> resultingRows = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
        ListenableFuture<List<TaskResult>> results = Futures.allAsList(executor.execute(job));
        Futures.addCallback(results, new FutureCallback<List<TaskResult>>() {
            @Override
            public void onSuccess(List<TaskResult> resultList) {
                for (Row row : resultList.get(0).rows()) {
                    resultingRows.add(row.materialize());
                }
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
                fail(t.getMessage());
            }
        });
        latch.await();

        assertThat(resultingRows.size(), is(13));
        assertThat(resultingRows.get(0).length, is(outputSymbols.size()));
        assertThat((Integer)resultingRows.get(0)[0], is(1));
        assertThat((Integer)resultingRows.get(12)[0], is(6));
    }
}