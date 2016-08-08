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

import io.crate.analyze.Analysis;
import io.crate.analyze.Analyzer;
import io.crate.analyze.ParameterContext;
import io.crate.core.collections.Bucket;
import io.crate.executor.transport.TransportExecutor;
import io.crate.planner.Plan;
import io.crate.planner.Planner;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.projection.FetchProjection;
import io.crate.sql.parser.SqlParser;
import io.crate.testing.CollectingRowReceiver;
import io.crate.testing.UseJdbc;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
@UseJdbc
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

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
        sqlExecutor.execBulk("insert into characters (id, name) values (?, ?)",
                new Object[][]{
                        new Object[]{1, "Arthur"},
                        new Object[]{2, "Ford"},
                }
        );
        sqlExecutor.exec("refresh table characters");
    }

    private Plan analyzeAndPlan(String stmt) {
        Analysis analysis = analyze(stmt);
        Planner planner = internalCluster().getInstance(Planner.class);
        return planner.plan(analysis, UUID.randomUUID(), 0, 0);
    }

    private Analysis analyze(String stmt) {
        Analyzer analyzer = internalCluster().getInstance(Analyzer.class);
        return analyzer.analyze(
                SqlParser.createStatement(stmt),
                ParameterContext.EMPTY
        );
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();

        Plan plan = analyzeAndPlan("select id, name, substr(name, 2) from characters order by id");
        assertThat(plan, instanceOf(QueryThenFetch.class));
        QueryThenFetch qtf = (QueryThenFetch) plan;

        assertThat(((FetchProjection) qtf.localMerge().projections().get(1)).nodeReaders(), notNullValue());
        assertThat(((FetchProjection) qtf.localMerge().projections().get(1)).readerIndices(), notNullValue());


        CollectingRowReceiver rowReceiver = new CollectingRowReceiver();
        executor.execute(plan, rowReceiver);

        Bucket result = rowReceiver.result();
        assertThat(result.size(), is(2));
        assertThat(rowReceiver.rows.get(0).length, is(3));
        assertThat((Integer) rowReceiver.rows.get(0)[0], is(1));
        assertThat((BytesRef) rowReceiver.rows.get(0)[1], is(new BytesRef("Arthur")));
        assertThat((BytesRef) rowReceiver.rows.get(0)[2], is(new BytesRef("rthur")));
        assertThat((Integer) rowReceiver.rows.get(1)[0], is(2));
        assertThat((BytesRef) rowReceiver.rows.get(1)[1], is(new BytesRef("Ford")));
        assertThat((BytesRef) rowReceiver.rows.get(1)[2], is(new BytesRef("ord")));
    }
}
