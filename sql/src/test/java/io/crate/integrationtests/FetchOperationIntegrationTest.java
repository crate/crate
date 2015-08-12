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

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.core.collections.Row;
import io.crate.executor.Job;
import io.crate.executor.TaskResult;
import io.crate.executor.transport.TransportExecutor;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.projection.FetchProjection;
import io.crate.sql.parser.SqlParser;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

@ElasticsearchIntegrationTest.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

    Setup setup = new Setup(sqlExecutor);
    TransportExecutor executor;

    @Before
    public void transportSetUp() {
        executor = internalCluster().getInstance(TransportExecutor.class);
    }

    @After
    public void transportTearDown() {
        executor = null;
    }

    private void setUpCharacters() {
        sqlExecutor.exec("create table characters (id int primary key, name string) " +
                "clustered into 2 shards with(number_of_replicas=0)");
        sqlExecutor.ensureYellowOrGreen();
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
        Planner planner = internalCluster().getInstance(Planner.class);
        return planner.plan(analysis, UUID.randomUUID());
    }

    private Analysis analyze(String stmt) {
        Analyzer analyzer = internalCluster().getInstance(Analyzer.class);
        return analyzer.analyze(
                SqlParser.createStatement(stmt),
                new ParameterContext(new Object[0], new Object[0][], null)
        );
    }

    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();

        Plan plan = analyzeAndPlan("select id, name, substr(name, 2) from characters order by id");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        QueryThenFetch qtf = (QueryThenFetch) plan;

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
}