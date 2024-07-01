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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.analyze.QueriedSelectRelation;
import io.crate.exceptions.OperationOnInaccessibleRelationException;
import io.crate.planner.CreateForeignTablePlan;
import io.crate.planner.CreateServerPlan;
import io.crate.planner.CreateUserMappingPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ForeignDataWrapperPlannerTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_creating_foreign_table_with_invalid_name_fails() throws Exception {
        SQLExecutor e = SQLExecutor.of(clusterService);

        assertThatThrownBy(() -> e.plan("create foreign table sys.nope (x int) server pg"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Cannot create relation in read-only schema: sys");
    }

    @Test
    public void test_create_server_fails_if_mandatory_options_are_missing() throws Exception {
        var e = SQLExecutor.of(clusterService);
        CreateServerPlan plan = e.plan("create server pg foreign data wrapper jdbc");
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Mandatory server option `url` for foreign data wrapper `jdbc` is missing");
    }

    @Test
    public void test_cannot_use_unsupported_options_in_create_server() throws Exception {
        var e = SQLExecutor.of(clusterService);
        CreateServerPlan plan = e.plan("create server pg foreign data wrapper jdbc options (url '', wrong_option 10)");
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage("Unsupported server options for foreign data wrapper `jdbc`: wrong_option. Valid options are: url");
    }

    @Test
    public void test_cannot_create_server_if_fdw_is_missing() throws Exception {
        var e = SQLExecutor.of(clusterService);
        String stmt = "create server pg foreign data wrapper dummy options (host 'localhost', dbname 'doc', port '5432')";
        CreateServerPlan plan = e.plan(stmt);
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .hasMessage("foreign-data wrapper dummy does not exist");
    }

    @Test
    public void test_cannot_add_foreign_table_with_invalid_options() throws Exception {
        Settings options = Settings.builder().put("url", "jdbc:postgresql://localhost:5432/").build();
        var e = SQLExecutor.of(clusterService)
            .addServer("pg", "jdbc", "crate", options);
        String stmt = "create foreign table tbl (x int) server pg options (invalid 42)";
        CreateForeignTablePlan plan = e.plan(stmt);
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .hasMessageContaining(
                "Unsupported options for foreign table doc.tbl using fdw `jdbc`: invalid. Valid options are: schema_name, table_name");
    }

    @Test
    public void test_cannot_create_user_mapping_with_invalid_options() throws Exception {
        Settings options = Settings.builder()
            .put("url", "jdbc:postgresql://localhost:5432/")
            .build();
        var e = SQLExecutor.of(clusterService)
            .addServer("pg", "jdbc", "crate", options);

        String stmt = "CREATE USER MAPPING FOR crate SERVER pg OPTIONS (\"option1\" 'abc');";
        CreateUserMappingPlan plan = e.plan(stmt);
        assertThatThrownBy(() -> e.execute(plan).getResult())
            .hasMessageContaining(
                "Invalid option 'option1' provided, the supported options are: [user, password]");
    }

    @Test
    public void test_show_create_table_on_foreign_table() throws Exception {
        Settings options = Settings.builder()
            .put("url", "jdbc:postgresql://localhost:5432/")
            .build();
        var e = SQLExecutor.of(clusterService)
            .addServer("pg", "jdbc", "crate", options)
            .addForeignTable("create foreign table tbl (x int) server pg options (schema_name 'doc')");

        List<Object[]> result = e.execute("show create table tbl").getResult();
        assertThat((String) result.get(0)[0]).startsWith(
            """
            CREATE FOREIGN TABLE IF NOT EXISTS "doc"."tbl" (
               "x" INTEGER
            ) SERVER pg OPTIONS (schema_name 'doc')
            """.stripIndent().trim()
        );
    }

    @Test
    public void test_doc_table_operations_raise_helpful_error_on_foreign_tables() throws Exception {
        Settings options = Settings.builder()
            .put("url", "jdbc:postgresql://localhost:5432/")
            .build();
        var e = SQLExecutor.of(clusterService)
            .addServer("pg", "jdbc", "crate", options)
            .addForeignTable("create foreign table doc.tbl (x int) server pg options (schema_name 'doc')");

        assertThatThrownBy(() -> e.plan("optimize table doc.tbl"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"doc.tbl\" doesn't support or allow OPTIMIZE operations");

        assertThatThrownBy(() -> e.plan("refresh table doc.tbl"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"doc.tbl\" doesn't support or allow REFRESH operations");

        assertThatThrownBy(() -> e.plan("create publication pub1 for table doc.tbl"))
            .isExactlyInstanceOf(OperationOnInaccessibleRelationException.class)
            .hasMessage("The relation \"doc.tbl\" doesn't support or allow CREATE PUBLICATION operations");
    }

    @Test
    public void test_subscript_on_ignored_object_leads_to_functioncall() throws Exception {
        // Remote end could be any database - CrateDB, PostgreSQL, etc, we don't know up-front
        //
        // Using `subscript(o, 'x')` as function instead of (Dynamic)Reference is
        // general and works in any case, but it bypasses push-down/optimizations of
        // `o['x']` if the remote is CrateDB
        //
        // Compromise: Allow `subscript(o, 'x')`, but only on `object(ignored)`.
        // It's a bit worse than native access on CrateDB (it fetches the full object),
        // but it at least makes no difference regarding tablescan/index-lookup as the
        // data is not indexed

        Settings options = Settings.builder()
            .put("url", "jdbc:postgresql://localhost:5432/")
            .build();
        var e = SQLExecutor.of(clusterService)
            .addServer("pg", "jdbc", "crate", options)
            .addForeignTable("create foreign table tbl (o object(ignored)) server pg options (schema_name 'doc')");

        QueriedSelectRelation relation = e.analyze("select o['x'] from tbl");
        assertThat(relation.outputs()).satisfiesExactly(
            x -> assertThat(x).isFunction("subscript")
        );
    }
}
