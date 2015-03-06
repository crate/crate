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

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.WhereClause;
import io.crate.core.collections.Row;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.NodeFetchRequest;
import io.crate.executor.transport.NodeFetchResponse;
import io.crate.executor.transport.TransportExecutor;
import io.crate.executor.transport.TransportFetchNodeAction;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.IterablePlan;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.test.integration.CrateIntegrationTest;
import io.crate.test.integration.CrateTestCluster;
import io.crate.types.IntegerType;
import io.crate.types.StringType;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.core.Is.is;

@CrateIntegrationTest.ClusterScope(scope = CrateIntegrationTest.Scope.GLOBAL)
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

    static {
        ClassLoader.getSystemClassLoader().setDefaultAssertionStatus(true);
    }

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


    @Test
    public void testCollectDocId() throws Exception {
        setUpCharacters();

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));
        Symbol docIdRef = new Reference(docIdRefInfo);
        Planner.Context plannerContext = new Planner.Context();

        CollectNode collectNode = new CollectNode("collect", tableInfo.getRouting(WhereClause.MATCH_ALL, null));
        collectNode.toCollect(Arrays.asList(docIdRef));
        collectNode.outputTypes(asList(docIdRefInfo.type()));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        plannerContext.allocateJobSearchContextIds(collectNode.routing());

        Plan plan = new IterablePlan(collectNode);
        Job job = executor.newJob(plan);

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

        TableInfo tableInfo = docSchemaInfo.getTableInfo("characters");
        ReferenceInfo docIdRefInfo = tableInfo.getReferenceInfo(new ColumnIdent("_docid"));
        Symbol docIdRef = new Reference(docIdRefInfo);
        Planner.Context plannerContext = new Planner.Context();

        CollectNode collectNode = new CollectNode("collect", tableInfo.getRouting(WhereClause.MATCH_ALL, null));
        collectNode.toCollect(Arrays.asList(docIdRef));
        collectNode.outputTypes(asList(docIdRefInfo.type()));
        collectNode.maxRowGranularity(RowGranularity.DOC);
        collectNode.closeContext(false);
        plannerContext.allocateJobSearchContextIds(collectNode.routing());

        Plan plan = new IterablePlan(collectNode);
        Job job = executor.newJob(plan);

        List<ListenableFuture<TaskResult>> result = executor.execute(job);

        // extract docIds by nodeId and jobSearchContextId
        Map<String, Map<Integer, IntArrayList>> jobSearchContextDocIds = new TreeMap<>();
        for (ListenableFuture<TaskResult> nodeResult : result) {
            TaskResult taskResult = nodeResult.get();
            long docId = (long)taskResult.rows().iterator().next().get(0);
            // unpack jobSearchContextId and reader doc id from docId
            int jobSearchContextId = (int)(docId >> 32);
            int doc = (int)docId;
            String nodeId = plannerContext.nodeId(jobSearchContextId);
            Map<Integer, IntArrayList> perNode = jobSearchContextDocIds.get(nodeId);
            if (perNode == null) {
                perNode = new TreeMap<>();
                jobSearchContextDocIds.put(nodeId, perNode);
            }
            IntArrayList docIds = perNode.get(jobSearchContextId);
            if (docIds == null) {
                docIds = new IntArrayList();
                perNode.put(jobSearchContextId, docIds);
            }
            docIds.add(doc);
        }

        TransportFetchNodeAction transportFetchNodeAction = cluster().getInstance(TransportFetchNodeAction.class);

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
        List<Symbol> toFetchSymbols = ImmutableList.<Symbol>of(new Reference(idRefInfo), new Reference(nameRefInfo));


        final CountDownLatch latch = new CountDownLatch(jobSearchContextDocIds.size());

        final List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, Map<Integer, IntArrayList>> nodeEntry : jobSearchContextDocIds.entrySet()) {
            NodeFetchRequest nodeFetchRequest = new NodeFetchRequest();
            nodeFetchRequest.jobId(collectNode.jobId().get());
            nodeFetchRequest.toFetchSymbols(toFetchSymbols);
            nodeFetchRequest.closeContext(true);
            for (Map.Entry<Integer, IntArrayList> entry : nodeEntry.getValue().entrySet()) {
                for (IntCursor cursor : entry.getValue()) {
                    nodeFetchRequest.addDocId(entry.getKey(), cursor.value);
                }
            }

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

}
