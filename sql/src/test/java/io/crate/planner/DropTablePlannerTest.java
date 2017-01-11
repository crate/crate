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
import io.crate.metadata.TableIdent;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.planner.node.ddl.GenericDDLPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DropTablePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() {
        e = SQLExecutor.builder(clusterService)
            .enableDefaultTables()
            .addBlobTable(TableDefinitions.createBlobTable(
                new TableIdent("blob", "screenshots"),
                clusterService))
            .build();
    }

    @Test
    public void testDropTable() throws Exception {
        DropTablePlan plan = e.plan("drop table users");
        assertThat(plan.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() throws Exception {
        Plan plan = e.plan("drop table if exists unknown_schema.unknwon_table");
        assertThat(plan, instanceOf(NoopPlan.class));
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        DropTablePlan plan = e.plan("drop table if exists users");
        assertThat(plan.tableInfo().ident().name(), is("users"));
    }

    @Test
    public void testDropTableIfExistsNonExistentTableCreatesNoop() throws Exception {
        Plan plan = e.plan("drop table if exists groups");
        assertThat(plan, instanceOf(NoopPlan.class));
    }


    @Test
    public void testDropPartitionedTable() throws Exception {
        DropTablePlan plan = e.plan("drop table parted");
        assertThat(plan.tableInfo().ident().name(), is("parted"));
    }

    @Test
    public void testDropBlobTableIfExistsCreatesIterablePlan() throws Exception {
        Plan plan = e.plan("drop blob table if exists screenshots");
        assertThat(plan, instanceOf(GenericDDLPlan.class));
    }

    @Test
    public void testDropNonExistentBlobTableCreatesNoop() throws Exception {
        Plan plan = e.plan("drop blob table if exists unknown");
        assertThat(plan, instanceOf(NoopPlan.class));
    }
}
