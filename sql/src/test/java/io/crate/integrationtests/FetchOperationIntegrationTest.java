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

import io.crate.data.Row;
import io.crate.planner.Merge;
import io.crate.planner.node.dql.QueryThenFetch;
import io.crate.planner.operators.LogicalPlan;
import io.crate.execution.dsl.projection.FetchProjection;
import io.crate.testing.TestingRowConsumer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 2, numClientNodes = 0)
public class FetchOperationIntegrationTest extends SQLTransportIntegrationTest {

    private void setUpCharacters() {
        execute("create table characters (id int primary key, name string) " +
                         "clustered into 2 shards with(number_of_replicas=0)");
        sqlExecutor.ensureYellowOrGreen();
        execute("insert into characters (id, name) values (?, ?)",
            new Object[][]{
                new Object[]{1, "Arthur"},
                new Object[]{2, "Ford"},
            }
        );
        execute("refresh table characters");
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testFetchProjection() throws Exception {
        setUpCharacters();
        PlanForNode plan = plan("select id, name, substr(name, 2) from characters order by id");
        QueryThenFetch qtf = (QueryThenFetch) ((LogicalPlan) plan.plan).build(
            plan.plannerContext, null, -1, 0, null, null, Row.EMPTY, Collections.emptyMap());
        assertThat(qtf.subPlan(), instanceOf(Merge.class));
        Merge merge = (Merge) qtf.subPlan();

        assertThat(((FetchProjection) merge.mergePhase().projections().get(0)).nodeReaders(), notNullValue());
        assertThat(((FetchProjection) merge.mergePhase().projections().get(0)).readerIndices(), notNullValue());

        TestingRowConsumer consumer = execute(plan);

        List<Object[]> result = consumer.getResult();
        assertThat(result.size(), is(2));
        assertThat(result.get(0).length, is(3));
        assertThat(result.get(0)[0], is(1));
        assertThat(result.get(0)[1], is(new BytesRef("Arthur")));
        assertThat(result.get(0)[2], is(new BytesRef("rthur")));
        assertThat(result.get(1)[0], is(2));
        assertThat(result.get(1)[1], is(new BytesRef("Ford")));
        assertThat(result.get(1)[2], is(new BytesRef("ord")));
    }
}
