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
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.action.job.ContextPreparer;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.breaker.RamAccountingContext;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.Row;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.NodeFetchRequest;
import io.crate.executor.transport.NodeFetchResponse;
import io.crate.executor.transport.TransportExecutor;
import io.crate.executor.transport.TransportFetchNodeAction;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.collect.MapSideDataCollectOperation;
import io.crate.operation.fetch.RowInputSymbolVisitor;
import io.crate.operation.projectors.CollectingProjector;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.consumer.ConsumerContext;
import io.crate.planner.consumer.QueryThenFetchConsumer;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.projection.FetchProjection;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.parser.SqlParser;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.test.integration.CrateTestCluster;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
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
    private static final RamAccountingContext ramAccountingContext =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

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
                        new Object[]{1, "Arthur"},
                        new Object[]{2, "Ford"},
                }
        );
        sqlExecutor.refresh("characters");
    }

    private Plan analyzeAndPlan(String stmt) {
        Analysis analysis = analyze(stmt);
        Planner planner = cluster().getInstance(Planner.class);
        return planner.plan(analysis);
    }

    private Analysis analyze(String stmt) {
        Analyzer analyzer = cluster().getInstance(Analyzer.class);
        return analyzer.analyze(
                SqlParser.createStatement(stmt),
                new ParameterContext(new Object[0], new Object[0][], null)
        );
    }

    private CollectNode createCollectNode(Planner.Context plannerContext, boolean keepContextForFetcher) {
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

        return collectNode;
    }

    @Test
    public void testCollectDocId() throws Exception {
        setUpCharacters();
        Planner.Context plannerContext = new Planner.Context(clusterService());
        CollectNode collectNode = createCollectNode(plannerContext, false);

        List<Bucket> results = new ArrayList<>();
        Iterable<MapSideDataCollectOperation> collectOperations = cluster().getInstances(MapSideDataCollectOperation.class);
        for (MapSideDataCollectOperation collectOperation : collectOperations) {
            List<JobExecutionContext> executionContexts = createJobContext(collectNode);

            CollectingProjector downstream = new CollectingProjector();
            collectOperation.collect(collectNode, downstream, ramAccountingContext);
            results.add(downstream.result().get());

            for (JobExecutionContext executionContext : executionContexts) {
                executionContext.close();
            }
        }

        assertThat(results.size(), is(2));
        int seenJobSearchContextId = -1;
        for (Bucket rows : results) {
            assertThat(rows.size(), is(1));
            Object docIdCol = rows.iterator().next().get(0);
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

        Analysis analysis = analyze("select id, name from characters");
        QueryThenFetchConsumer queryThenFetchConsumer = cluster().getInstance(QueryThenFetchConsumer.class);
        Planner.Context plannerContext = new Planner.Context(clusterService());
        ConsumerContext consumerContext = new ConsumerContext(analysis.rootRelation(), plannerContext);
        queryThenFetchConsumer.consume(analysis.rootRelation(), consumerContext);

        QueryThenFetch plan = ((QueryThenFetch) ((PlannedAnalyzedRelation) consumerContext.rootRelation()).plan());
        UUID jobId = UUID.randomUUID();
        plan.collectNode().jobId(jobId);
        Iterable<MapSideDataCollectOperation> collectOperations = cluster().getInstances(MapSideDataCollectOperation.class);

        createJobContext(plan.collectNode());

        List<Bucket> results = new ArrayList<>();
        for (MapSideDataCollectOperation collectOperation : collectOperations) {

            CollectingProjector collectingProjector = new CollectingProjector();
            collectOperation.collect(plan.collectNode(), collectingProjector, ramAccountingContext);
            results.add(collectingProjector.result().get());
        }

        TransportFetchNodeAction transportFetchNodeAction = cluster().getInstance(TransportFetchNodeAction.class);

        // extract docIds by nodeId and jobSearchContextId
        Map<String, LongArrayList> jobSearchContextDocIds = new HashMap<>();
        for (Bucket rows : results) {
            long docId = (long)rows.iterator().next().get(0);
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

        Iterable<Projection> projections = Iterables.filter(plan.mergeNode().projections(), Predicates.instanceOf(FetchProjection.class));
        FetchProjection fetchProjection = (FetchProjection )Iterables.getOnlyElement(projections);
        RowInputSymbolVisitor rowInputSymbolVisitor = new RowInputSymbolVisitor(cluster().getInstance(Functions.class));
        RowInputSymbolVisitor.Context context = rowInputSymbolVisitor.extractImplementations(fetchProjection.outputSymbols());


        final CountDownLatch latch = new CountDownLatch(jobSearchContextDocIds.size());
        final List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, LongArrayList> nodeEntry : jobSearchContextDocIds.entrySet()) {
            NodeFetchRequest nodeFetchRequest = new NodeFetchRequest();
            nodeFetchRequest.jobId(plan.collectNode().jobId());
            nodeFetchRequest.executionNodeId(plan.collectNode().executionNodeId());
            nodeFetchRequest.toFetchReferences(context.references());
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
        for (Row row : rows) {
            assertThat((Integer) row.get(0), anyOf(is(1), is(2)));
            assertThat((BytesRef) row.get(1), anyOf(is(new BytesRef("Arthur")), is(new BytesRef("Ford"))));
        }
    }

    private List<JobExecutionContext> createJobContext(CollectNode collectNode) {
        ContextPreparer contextPreparer = cluster().getInstance(ContextPreparer.class);

        List<JobExecutionContext> executionContexts = new ArrayList<>(2);
        for (JobContextService jobContextService : cluster().getInstances(JobContextService.class)) {
            JobExecutionContext.Builder builder = jobContextService.newBuilder(collectNode.jobId());
            contextPreparer.prepare(collectNode.jobId(), collectNode, builder);
            executionContexts.add(jobContextService.createContext(builder));
        }
        return executionContexts;
    }

    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();

        Plan plan = analyzeAndPlan("select id, name, substr(name, 2) from characters order by id");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        QueryThenFetch qtf = (QueryThenFetch) plan;

        assertThat(qtf.collectNode().keepContextForFetcher(), is(true));
        assertThat(((FetchProjection) qtf.mergeNode().projections().get(1)).jobSearchContextIdToNode(), notNullValue());
        assertThat(((FetchProjection) qtf.mergeNode().projections().get(1)).jobSearchContextIdToShard(), notNullValue());

        Job job = executor.newJob(plan);
        ListenableFuture<List<TaskResult>> results = Futures.allAsList(executor.execute(job));

        final List<Object[]> resultingRows = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
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
        assertThat(resultingRows.get(0).length, is(3));
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

        Plan plan = analyzeAndPlan("select position, name from locations order by position");
        assertThat(plan, instanceOf(QueryThenFetch.class));

        rewriteFetchProjectionToBulkSize(bulkSize, ((QueryThenFetch) plan).mergeNode());

        Job job = executor.newJob(plan);
        ListenableFuture<List<TaskResult>> results = Futures.allAsList(executor.execute(job));

        final List<Object[]> resultingRows = new ArrayList<>();
        final CountDownLatch latch = new CountDownLatch(1);
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
        assertThat(resultingRows.get(0).length, is(2));
        assertThat((Integer) resultingRows.get(0)[0], is(1));
        assertThat((Integer) resultingRows.get(12)[0], is(6));
    }

    private void rewriteFetchProjectionToBulkSize(int bulkSize, MergeNode mergeNode) {
        List<Projection> newProjections = new ArrayList<>(mergeNode.projections().size());
        for (Projection projection : mergeNode.projections()) {
            if (projection instanceof FetchProjection) {
                FetchProjection fetchProjection = (FetchProjection) projection;
                newProjections.add(new FetchProjection(
                        fetchProjection.executionNodeId(),
                        fetchProjection.docIdSymbol(),
                        fetchProjection.inputSymbols(),
                        fetchProjection.outputSymbols(),
                        fetchProjection.partitionedBy(),
                        fetchProjection.executionNodes(),
                        bulkSize,
                        fetchProjection.closeContexts(),
                        fetchProjection.jobSearchContextIdToNode(),
                        fetchProjection.jobSearchContextIdToShard()));
            } else {
                newProjections.add(projection);
            }
        }
        mergeNode.projections(newProjections);
    }
}