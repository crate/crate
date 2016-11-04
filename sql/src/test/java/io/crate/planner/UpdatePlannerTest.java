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

package io.crate.planner;

import io.crate.analyze.TableDefinitions;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Reference;
import io.crate.planner.node.dml.Upsert;
import io.crate.planner.node.dml.UpsertById;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.MergePhase;
import io.crate.planner.node.dql.RoutedCollectPhase;
import io.crate.planner.projection.MergeCountProjection;
import io.crate.planner.projection.UpdateProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;

public class UpdatePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addDocTable(TableDefinitions.PARTED_PKS_TI)
            .addDocTable(TableDefinitions.TEST_EMPTY_PARTITIONED_TABLE_INFO)
            .build();
    }

    @Test
    public void testUpdateByQueryPlan() throws Exception {
        Upsert plan = e.plan("update users set name='Vogon lyric fan'");
        assertThat(plan.nodes().size(), is(1));

        Merge merge = (Merge) plan.nodes().get(0);
        Collect collect = (Collect) merge.subPlan();

        RoutedCollectPhase collectPhase = ((RoutedCollectPhase) collect.collectPhase());
        assertThat(collectPhase.routing(), is(TableDefinitions.SHARD_ROUTING));
        assertFalse(collectPhase.whereClause().noMatch());
        assertFalse(collectPhase.whereClause().hasQuery());
        assertThat(collectPhase.projections().size(), is(1));
        assertThat(collectPhase.projections().get(0), instanceOf(UpdateProjection.class));
        assertThat(collectPhase.toCollect().size(), is(1));
        assertThat(collectPhase.toCollect().get(0), instanceOf(Reference.class));
        assertThat(((Reference) collectPhase.toCollect().get(0)).ident().columnIdent().fqn(), is("_id"));

        UpdateProjection updateProjection = (UpdateProjection) collectPhase.projections().get(0);
        assertThat(updateProjection.uidSymbol(), instanceOf(InputColumn.class));

        assertThat(updateProjection.assignmentsColumns()[0], is("name"));
        Symbol symbol = updateProjection.assignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));

        MergePhase mergePhase = merge.mergePhase();
        assertThat(mergePhase.projections().size(), is(1));
        assertThat(mergePhase.projections().get(0), instanceOf(MergeCountProjection.class));

        assertThat(mergePhase.outputTypes().size(), is(1));
    }

    @Test
    public void testUpdateByIdPlan() throws Exception {
        UpsertById upsertById = e.plan("update users set name='Vogon lyric fan' where id=1");
        assertThat(upsertById.items().size(), is(1));

        assertThat(upsertById.updateColumns()[0], is("name"));

        UpsertById.Item item = upsertById.items().get(0);
        assertThat(item.index(), is("users"));
        assertThat(item.id(), is("1"));

        Symbol symbol = item.updateAssignments()[0];
        assertThat(symbol, isLiteral("Vogon lyric fan", DataTypes.STRING));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        UpsertById plan = e.plan("update users set name='Vogon lyric fan' where id in (1,2,3)");

        List<String> ids = new ArrayList<>(3);
        for (UpsertById.Item item : plan.items()) {
            ids.add(item.id());
            assertThat(item.updateAssignments().length, is(1));
            assertThat(item.updateAssignments()[0], isLiteral("Vogon lyric fan", DataTypes.STRING));
        }

        assertThat(ids, containsInAnyOrder("1", "2", "3"));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValuesPartitioned() throws Exception {
        UpsertById planNode = e.plan("update parted_pks set name='Vogon lyric fan' where " +
                                     "(id=2 and date = 0) OR" +
                                     "(id=3 and date=123)");

        List<String> partitions = new ArrayList<>(2);
        List<String> ids = new ArrayList<>(2);
        for (UpsertById.Item item : planNode.items()) {
            partitions.add(item.index());
            ids.add(item.id());
            assertThat(item.updateAssignments().length, is(1));
            assertThat(item.updateAssignments()[0], isLiteral("Vogon lyric fan", DataTypes.STRING));
        }
        assertThat(ids, containsInAnyOrder("AgEyATA=", "AgEzAzEyMw==")); // multi primary key - values concatenated and base64'ed
        assertThat(partitions, containsInAnyOrder(".partitioned.parted_pks.04130", ".partitioned.parted_pks.04232chj"));
    }

    @Test
    public void testUpdateOnEmptyPartitionedTable() throws Exception {
        Plan plan = e.plan("update empty_parted set name='Vogon lyric fan'");
        assertThat(plan, instanceOf(NoopPlan.class));
    }
}
