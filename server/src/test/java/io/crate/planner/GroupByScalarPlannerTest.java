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

package io.crate.planner;

import static io.crate.testing.Asserts.assertThat;
import static io.crate.testing.Asserts.isReference;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.EvalProjection;
import io.crate.execution.dsl.projection.GroupProjection;
import io.crate.expression.symbol.Function;
import io.crate.metadata.RowGranularity;
import io.crate.planner.node.dql.Collect;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;

public class GroupByScalarPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.builder(clusterService)
            .setNumNodes(2)
            .build()
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
    }

    @Test
    public void testGroupByWithScalarPlan() throws Exception {
        Merge merge = e.plan("SELECT id + 1 FROM users GROUP BY id");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertThat(collectPhase.outputTypes().get(0)).isEqualTo(DataTypes.LONG);
        assertThat(collectPhase.maxRowGranularity()).isEqualTo(RowGranularity.DOC);
        assertThat(collectPhase.projections()).hasSize(2);
        assertThat(collectPhase.projections().get(0)).isExactlyInstanceOf(GroupProjection.class);
        assertThat(collectPhase.projections().get(0).requiredGranularity()).isEqualTo(RowGranularity.SHARD);
        assertThat(collectPhase.projections().get(1)).isExactlyInstanceOf(EvalProjection.class);
        assertThat(collectPhase.projections().get(1).outputs().get(0)).isExactlyInstanceOf(Function.class);
        assertThat(collectPhase.toCollect()).satisfiesExactly(isReference("id", DataTypes.LONG));

        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.keys().get(0).valueType()).isEqualTo(DataTypes.LONG);


        assertThat(collectPhase.projections().get(1).outputs())
            .satisfiesExactly(s -> assertThat(s).isFunction("add"));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.inputTypes().iterator().next()).isEqualTo(DataTypes.LONG);
        assertThat(mergePhase.outputTypes().get(0)).isEqualTo(DataTypes.LONG);
    }

    @Test
    public void testGroupByWithMultipleScalarPlan() throws Exception {
        Merge merge = e.plan("SELECT abs(id + 1) FROM users GROUP BY id");
        Collect collect = (Collect) merge.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());

        assertThat(collectPhase.outputTypes().get(0)).isEqualTo(DataTypes.LONG);
        assertThat(collectPhase.maxRowGranularity()).isEqualTo(RowGranularity.DOC);
        assertThat(collectPhase.projections()).hasSize(2);
        assertThat(collectPhase.projections().get(0)).isExactlyInstanceOf(GroupProjection.class);
        assertThat(collectPhase.projections().get(0).requiredGranularity()).isEqualTo(RowGranularity.SHARD);
        assertThat(collectPhase.projections().get(1)).isExactlyInstanceOf(EvalProjection.class);
        assertThat(collectPhase.projections().get(1).outputs().get(0)).isFunction("abs");
        assertThat(collectPhase.toCollect()).satisfiesExactly(isReference("id", DataTypes.LONG));

        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.keys().get(0).valueType()).isEqualTo(DataTypes.LONG);

        MergePhase mergePhase = merge.mergePhase();

        assertThat(mergePhase.inputTypes().iterator().next()).isEqualTo(DataTypes.LONG);
        assertThat(mergePhase.outputTypes().get(0)).isEqualTo(DataTypes.LONG);
    }

    @Test
    public void testGroupByScalarWithMultipleColumnArgumentsPlan() throws Exception {
        Merge merge = e.plan("SELECT abs(id + other_id) FROM users GROUP BY id, other_id");
        Merge subplan = (Merge) merge.subPlan();
        Collect collect = (Collect) subplan.subPlan();
        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.projections()).hasSize(1);
        assertThat(collectPhase.projections().get(0)).isExactlyInstanceOf(GroupProjection.class);
        assertThat(collectPhase.projections().get(0).requiredGranularity()).isEqualTo(RowGranularity.SHARD);
        assertThat(collectPhase.toCollect()).satisfiesExactly(
            isReference("id", DataTypes.LONG), isReference("other_id", DataTypes.LONG));

        GroupProjection groupProjection = (GroupProjection) collectPhase.projections().get(0);
        assertThat(groupProjection.keys()).hasSize(2);
        assertThat(groupProjection.keys().get(0).valueType()).isEqualTo(DataTypes.LONG);
        assertThat(groupProjection.keys().get(1).valueType()).isEqualTo(DataTypes.LONG);

        MergePhase mergePhase = subplan.mergePhase();
        assertThat(mergePhase.projections()).hasSize(2);
        assertThat(mergePhase.projections().get(0)).isExactlyInstanceOf(GroupProjection.class);
        assertThat(mergePhase.projections().get(1)).isExactlyInstanceOf(EvalProjection.class);

        assertThat(mergePhase.projections().get(1).outputs())
            .satisfiesExactly(s -> assertThat(s).isFunction("abs"));
    }

    @Test
    public void test_group_by_scalar_containing_a_table_function_results_in_project_set() {
        var logicalPlan = e.logicalPlan("SELECT regexp_matches(name, '.*')[1] FROM users GROUP BY 1");
        var expectedPlan =
            """
            GroupHashAggregate[regexp_matches(name, '.*')[1]]
              └ ProjectSet[regexp_matches(name, '.*'), name]
                └ Collect[doc.users | [name] | true]
            """;
        assertThat(logicalPlan).isEqualTo(expectedPlan);
    }
}
