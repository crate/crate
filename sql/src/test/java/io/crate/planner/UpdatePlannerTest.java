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
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.data.Row;
import io.crate.metadata.Reference;
import io.crate.planner.consumer.UpdatePlanner;
import io.crate.planner.node.dml.UpdateById;
import io.crate.planner.node.dql.Collect;
import io.crate.execution.dsl.phases.MergePhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.execution.dsl.projection.MergeCountProjection;
import io.crate.execution.dsl.projection.UpdateProjection;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.DataTypes;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.contains;
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
        UpdatePlanner.Update plan = e.plan("update users set name='Vogon lyric fan'");
        Merge merge = (Merge) plan.createExecutionPlan.create(
            e.getPlannerContext(clusterService.state()), Row.EMPTY, emptyMap());

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
        UpdateById updateById = e.plan("update users set name='Vogon lyric fan' where id=1");

        assertThat(updateById.assignmentByTargetCol().keySet(), contains(isReference("name")));
        assertThat(updateById.assignmentByTargetCol().values(), contains(isLiteral("Vogon lyric fan")));
        assertThat(updateById.docKeys().size(), is(1));

        assertThat(updateById.docKeys().getOnlyKey().getId(e.functions(), Row.EMPTY, emptyMap()), is("1"));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValues() throws Exception {
        UpdateById update = e.plan("update users set name='Vogon lyric fan' where id in (1,2,3)");
        assertThat(update.docKeys().size(), is(3));
    }

    @Test
    public void testUpdatePlanWithMultiplePrimaryKeyValuesPartitioned() throws Exception {
        Plan update = e.plan("update parted_pks set name='Vogon lyric fan' where " +
                                     "(id=2 and date = 0) OR" +
                                     "(id=3 and date=123)");
        assertThat(update, instanceOf(UpdateById.class));
        assertThat(((UpdateById) update).docKeys().size(), is(2));
    }

    @Test
    public void testUpdateOnEmptyPartitionedTable() throws Exception {
        UpdatePlanner.Update update = e.plan("update empty_parted set name='Vogon lyric fan'");
        Collect collect = (Collect) update.createExecutionPlan.create(
            e.getPlannerContext(clusterService.state()), Row.EMPTY, emptyMap());
        assertThat(((RoutedCollectPhase) collect.collectPhase()).routing().nodes(), Matchers.emptyIterable());
    }
}
