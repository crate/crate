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

import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.WhereClause;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportExecutor;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocSchemaInfo;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Arrays.asList;
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


    @Test
    public void testCollectDocId() throws Exception {
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
            assertThat(taskResult.rows().length, is(1));
            assertNotNull(taskResult.rows()[0][0]);
            assertThat(taskResult.rows()[0][0], instanceOf(Long.class));
            long docId = (long)taskResult.rows()[0][0];
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


}
