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

package io.crate.fdw;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.junit.Test;

import io.crate.planner.CreateForeignTablePlan;
import io.crate.planner.CreateServerPlan;
import io.crate.planner.CreateUserMappingPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ForeignDataWrapperPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_creating_foreign_table_with_invalid_name_fails() throws Exception {
        SQLExecutor e = SQLExecutor.builder(clusterService)
            .build();

        assertThatThrownBy(() -> e.plan("create foreign table sys.nope (x int) server pg"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot create relation in read-only schema: sys");
    }

    @Test
    public void test_create_server_fails_if_mandatory_options_are_missing() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        CreateServerPlan plan = e.plan("create server pg foreign data wrapper jdbc");
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Mandatory server option `url` for foreign data wrapper `jdbc` is missing");
    }

    @Test
    public void test_cannot_use_unsupported_options_in_create_server() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        CreateServerPlan plan = e.plan("create server pg foreign data wrapper jdbc options (url '', wrong_option 10)");
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsupported server options for foreign data wrapper `jdbc`: wrong_option. Valid options are: url");
    }

    @Test
    public void test_cannot_create_server_if_fdw_is_missing() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        String stmt = "create server pg foreign data wrapper dummy options (host 'localhost', dbname 'doc', port '5432')";
        CreateServerPlan plan = e.plan(stmt);
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .hasMessage("foreign-data wrapper dummy does not exist");
    }

    @Test
    public void test_cannot_add_foreign_table_with_invalid_options() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        CreateServerRequest request = new CreateServerRequest(
            "pg",
            "jdbc",
            "crate",
            true,
            Settings.builder().put("url", "jdbc:postgresql://localhost:5432/").build()
        );
        AddServerTask addServerTask = new AddServerTask(e.foreignDataWrappers, request);
        ClusterServiceUtils.setState(clusterService, addServerTask.execute(clusterService.state()));

        String stmt = "create foreign table tbl (x int) server pg options (invalid 42)";
        CreateForeignTablePlan plan = e.plan(stmt);
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .hasMessageContaining(
                "Unsupported options for foreign table doc.tbl using fdw `jdbc`: invalid. Valid options are: schema_name, table_name");
    }

    @Test
    public void test_cannot_create_user_mapping_with_invalid_options() throws Exception {
        var e = SQLExecutor.builder(clusterService).build();
        CreateServerRequest request = new CreateServerRequest(
            "pg",
            "jdbc",
            "crate",
            true,
            Settings.builder().put("url", "jdbc:postgresql://localhost:5432/").build()
        );
        AddServerTask addServerTask = new AddServerTask(e.foreignDataWrappers, request);
        ClusterServiceUtils.setState(clusterService, addServerTask.execute(clusterService.state()));

        String stmt = "CREATE USER MAPPING FOR crate SERVER pg OPTIONS (\"option1\" 'abc');";
        CreateUserMappingPlan plan = e.plan(stmt);
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .hasMessageContaining(
                "Invalid option 'option1' provided, the supported options are: [user, password]");
    }
}
