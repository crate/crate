/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.planner.consumer;

import io.crate.analyze.TableDefinitions;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.execution.dsl.projection.TopNProjection;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.join.Join;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.isInputColumn;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;

public class GlobalAggregatePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(T3.T1_INFO)
            .build();
    }

    @Test
    public void testAggregateOnSubQueryHasNoFilterProjectionWithoutWhereAndHaving() throws Exception {
        Collect plan = e.plan("select sum(x) from (select x from t1 order by x limit 10) ti");
        for (Projection projection : plan.collectPhase().projections()) {
            assertThat(projection, Matchers.not(instanceOf(FilterProjection.class)));
        }
    }

    @Test
    public void testAggregateOnSubQueryNoFetchBecauseColumnInUse() throws Exception {
        Collect plan = e.plan("select sum(x) from (select x, i from t1 order by x limit 10) ti");
        List<Projection> projections = plan.collectPhase().projections();
        assertThat(projections, contains(
            instanceOf(TopNProjection.class),
            instanceOf(AggregationProjection.class)
        ));
    }

    @Test
    @Ignore("TODO: figure out if early output stripping is necessary")
    public void testJoinConditionFieldsAreNotPartOfNLOutputOnAggOnJoin() throws Exception {
        Join nl = e.plan("select sum(u1.ints) from users u1 " +
                         "    inner join users u2 on u1.id = u2.id ");
        List<Projection> projections = nl.joinPhase().projections();
        assertThat(projections, contains(
            instanceOf(EvalProjection.class),
            instanceOf(AggregationProjection.class)
        ));
        // Only u1.ints is in the outputs of the NL (pre projections)
        assertThat(projections.get(0).outputs(), contains(isInputColumn(1)));
    }
}
