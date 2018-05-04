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

package io.crate.planner.operators;

import com.carrotsearch.hppc.ObjectLongHashMap;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.TableDefinitions;
import io.crate.data.Row;
import io.crate.execution.dsl.projection.builder.ProjectionBuilder;
import io.crate.metadata.Functions;
import io.crate.metadata.TableIdent;
import io.crate.planner.PlannerContext;
import io.crate.planner.SubqueryPlanner;
import io.crate.planner.TableStats;
import io.crate.planner.distribution.DistributionType;
import io.crate.planner.node.dql.Collect;
import io.crate.planner.node.dql.join.NestedLoop;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static io.crate.testing.TestingHelpers.getFunctions;
import static io.crate.testing.TestingHelpers.isSQL;
import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.notNullValue;

public class JoinTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;
    private Functions functions = getFunctions();
    private ProjectionBuilder projectionBuilder = new ProjectionBuilder(functions);

    private PlannerContext plannerCtx;

    @Before
    public void setUpExecutor() throws Exception {
        e = SQLExecutor.builder(clusterService)
            .addDocTable(TableDefinitions.USER_TABLE_INFO)
            .addDocTable(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_INFO)
            .build();
        plannerCtx = e.getPlannerContext(clusterService.state());
    }

    private LogicalPlan createLogicalPlan(MultiSourceSelect mss, TableStats tableStats) {
        LogicalPlanner logicalPlanner = new LogicalPlanner(functions, tableStats);
        SubqueryPlanner subqueryPlanner = new SubqueryPlanner((s) -> logicalPlanner.planSubSelect(s, plannerCtx));
        return Join.createNodes(mss, mss.where(), subqueryPlanner).build(tableStats, Collections.emptySet());
    }

    private NestedLoop buildJoin(LogicalPlan operator) {
        return (NestedLoop) operator.build(plannerCtx, projectionBuilder, -1, 0, null, null, Row.EMPTY, emptyMap());
    }

    private NestedLoop plan(MultiSourceSelect mss, TableStats tableStats) {
        return buildJoin(createLogicalPlan(mss, tableStats));
    }

    @Test
    public void testNestedLoop_TablesAreSwitchedIfLeftIsSmallerThanRight() throws Exception {
        MultiSourceSelect mss = e.analyze("select users.id from users, locations where users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectLongHashMap<TableIdent> rowCountByTable = new ObjectLongHashMap<>();
        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, 10);
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, 10_000);
        tableStats.updateTableStats(rowCountByTable);

        NestedLoop nl = plan(mss, tableStats);
        assertThat(
            ((Collect) nl.left()).collectPhase().distributionInfo().distributionType(),
            is(DistributionType.BROADCAST)
        );

        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, 10_000);
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, 10);
        tableStats.updateTableStats(rowCountByTable);

        nl = plan(mss, tableStats);
        assertThat(
            ((Collect) nl.left()).collectPhase().distributionInfo().distributionType(),
            is(DistributionType.SAME_NODE)
        );
    }

    @Test
    public void testNestedLoop_TablesAreNotSwitchedIfLeftHasAPushedDownOrderBy() {
        // we use a subselect to simulate the pushed-down order by
        MultiSourceSelect mss = e.analyze("select users.id from (select id from users order by id) users, " +
                                          "locations where users.id = locations.id");

        TableStats tableStats = new TableStats();
        ObjectLongHashMap<TableIdent> rowCountByTable = new ObjectLongHashMap<>();
        rowCountByTable.put(TableDefinitions.USER_TABLE_IDENT, 10);
        rowCountByTable.put(TableDefinitions.TEST_DOC_LOCATIONS_TABLE_IDENT, 10_000);
        tableStats.updateTableStats(rowCountByTable);

        NestedLoop nl = plan(mss, tableStats);

        assertThat(((Collect) nl.left()).collectPhase().toCollect(), isSQL("doc.users.id"));
        assertThat(nl.resultDescription().orderBy(), notNullValue());
    }
}
