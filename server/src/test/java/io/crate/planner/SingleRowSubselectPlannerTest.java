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

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.operators.RootRelationBoundary;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.testing.T3;

public class SingleRowSubselectPlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(T3.T1_DEFINITION)
            .addTable(T3.T2_DEFINITION)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION);
    }

    @Test
    public void testPlanSimpleSelectWithSingleRowSubSelectInWhereClause() throws Exception {
        RootRelationBoundary plan = e.logicalPlan("select x from t1 where a = (select b from t2)");
        assertThat(plan.dependencies().keySet(), contains(instanceOf(RootRelationBoundary.class)));
    }

    @Test
    public void testPlanSelectOnSysTablesWithSingleRowSubselectInWhere() throws Exception {
        LogicalPlan plan = e.logicalPlan("select name from sys.cluster where name = (select 'foo')");
        assertThat(plan.dependencies().keySet(), contains(instanceOf(RootRelationBoundary.class)));
    }

    @Test
    public void testSingleRowSubSelectInSelectList() throws Exception {
        LogicalPlan plan = e.logicalPlan("select (select b from t2 limit 1) from t1");
        assertThat(plan.dependencies().keySet(), contains(instanceOf(RootRelationBoundary.class)));
    }

    @Test
    public void testSingleRowSubSelectAndDocKeysInWhereClause() throws Exception {
        LogicalPlan plan = e.logicalPlan("select (select 'foo' from sys.cluster) from users where id = 10");
        assertThat(plan.dependencies().keySet(), contains(instanceOf(RootRelationBoundary.class)));
    }

    @Test
    public void testSingleRowSubSelectOfWhereInJoin() throws Exception {
        LogicalPlan plan = e.logicalPlan("select * from users u1, users u2 where u1.name = (select 'Arthur')");
        assertThat(plan.dependencies().keySet(), contains(instanceOf(RootRelationBoundary.class)));
    }
}
