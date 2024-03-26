/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.consumer;

import static io.crate.testing.Asserts.isInputColumn;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.data.Row1;
import io.crate.execution.dsl.projection.AggregationProjection;
import io.crate.execution.dsl.projection.FilterProjection;
import io.crate.execution.dsl.projection.LimitAndOffsetProjection;
import io.crate.execution.dsl.projection.Projection;
import io.crate.metadata.RowGranularity;
import io.crate.planner.node.dql.Collect;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.Asserts;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class GlobalAggregatePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addTable(T3.T1_DEFINITION);
    }

    @Test
    public void testAggregateOnSubQueryHasNoFilterProjectionWithoutWhereAndHaving() throws Exception {
        Collect plan = e.plan("select sum(x) from (select x from t1 order by x limit 10) ti");
        for (Projection projection : plan.collectPhase().projections()) {
            assertThat(projection).isNotInstanceOf(FilterProjection.class);
        }
    }

    @Test
    public void testAggregateOnSubQueryNoFetchBecauseColumnInUse() throws Exception {
        Collect plan = e.plan("select sum(x) from (select x, i from t1 order by x limit 10) ti");
        List<Projection> projections = plan.collectPhase().projections();
        assertThat(projections)
            .satisfiesExactly(
                s -> assertThat(s).isExactlyInstanceOf(LimitAndOffsetProjection.class),
                s -> assertThat(s).isExactlyInstanceOf(AggregationProjection.class)
            );
    }

    @Test
    public void test_aggregation_is_correctly_build_with_parameterized_expression() {
        Collect plan = e.plan("select sum(x + ?) from t1", UUID.randomUUID(), 0, new Row1(1));
        var projections = plan.collectPhase().projections();
        assertThat(projections)
            .satisfiesExactly(
                s -> assertThat(s).isExactlyInstanceOf(AggregationProjection.class),
                s -> assertThat(s).isExactlyInstanceOf(AggregationProjection.class)
            );
        //noinspection unchecked
        assertThat(projections.get(0).outputs())
            .satisfiesExactly(
                a -> Asserts.assertThat(a).isAggregation("sum", isInputColumn(0))
            );
        //noinspection unchecked
        assertThat(projections.get(1).outputs())
            .satisfiesExactly(
                a -> Asserts.assertThat(a).isAggregation("sum", isInputColumn(0))
            );
    }

    @Test
    public void test_aggregate_on_virtual_table_uses_shard_projections_if_possible() {
        Collect plan = e.plan("select sum(x) from (select x from t1) t");
        List<Projection> projections = plan.collectPhase().projections();
        assertThat(projections)
            .satisfiesExactly(
                s -> assertThat(s).isExactlyInstanceOf(AggregationProjection.class),
                s -> assertThat(s).isExactlyInstanceOf(AggregationProjection.class)
            );
        assertThat(projections.get(0).requiredGranularity()).isEqualTo(RowGranularity.SHARD);
    }
}
