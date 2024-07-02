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

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import io.crate.analyze.TableDefinitions;
import io.crate.metadata.RelationName;
import io.crate.planner.node.ddl.DropTablePlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class DropTablePlannerTest extends CrateDummyClusterServiceUnitTest {

    private SQLExecutor e;

    @Before
    public void prepare() throws IOException {
        e = SQLExecutor.of(clusterService)
            .addTable(TableDefinitions.USER_TABLE_DEFINITION)
            .addPartitionedTable(
                TableDefinitions.TEST_PARTITIONED_TABLE_DEFINITION,
                TableDefinitions.TEST_PARTITIONED_TABLE_PARTITIONS)
            .addBlobTable("create blob table screenshots");
    }

    @Test
    public void testDropTable() throws Exception {
        DropTablePlan plan = e.plan("drop table users");
        assertThat(plan.dropTable().tableName().name()).isEqualTo("users");
    }

    @Test
    public void testDropTableIfExistsWithUnknownSchema() throws Exception {
        DropTablePlan plan = e.plan("drop table if exists unknown_schema.unknown_table");
        assertThat(plan.dropTable().tableName()).isEqualTo(new RelationName("unknown_schema", "unknown_table"));
    }

    @Test
    public void testDropTableIfExists() throws Exception {
        DropTablePlan plan = e.plan("drop table if exists users");
        assertThat(plan.dropTable().tableName().name()).isEqualTo("users");
    }

    @Test
    public void testDropTableIfExistsNonExistentTableCreatesPlanWithoutTableInfo() throws Exception {
        DropTablePlan plan = e.plan("drop table if exists groups");
        assertThat(plan.dropTable().tableName().name()).isEqualTo("groups");
    }


    @Test
    public void testDropPartitionedTable() throws Exception {
        DropTablePlan plan = e.plan("drop table parted");
        assertThat(plan.dropTable().tableName().name()).isEqualTo("parted");
    }

    @Test
    public void testDropBlobTableIfExistsCreatesDropTablePlan() throws Exception {
        Plan plan = e.plan("drop blob table if exists screenshots");
        assertThat(plan).isExactlyInstanceOf(DropTablePlan.class);
    }

    @Test
    public void testDropNonExistentBlobTableCreatesPlanWithoutTableInfo() throws Exception {
        DropTablePlan plan = e.plan("drop blob table if exists unknown");
        assertThat(plan.dropTable().tableName().name()).isEqualTo("unknown");
    }
}
