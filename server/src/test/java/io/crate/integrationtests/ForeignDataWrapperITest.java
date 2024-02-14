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

package io.crate.integrationtests;

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.protocols.postgres.PostgresNetty;
import io.crate.role.Role;
import io.crate.role.Roles;

public class ForeignDataWrapperITest extends IntegTestCase {

    @After
    public void removeServers() throws Exception {
        execute("drop server if exists pg cascade");
        execute("drop user if exists trillian");
        execute("drop user if exists arthur");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("fdw.allow_local", true)
            .build();
    }

    @Test
    public void test_cannot_create_server_if_fdw_is_missing() throws Exception {
        String stmt = "create server pg foreign data wrapper dummy options (host 'localhost', dbname 'doc', port '5432')";
        assertThatThrownBy(() -> execute(stmt))
            .hasMessageContaining("foreign-data wrapper dummy does not exist");
    }

    @Test
    public void test_full_fdw_flow() throws Exception {
        execute("create user trillian with (password = 'secret')");
        execute("create user arthur with (password = 'not-so-secret')");

        execute("create table doc.tbl (x int)");
        execute("insert into doc.tbl (x) values (1), (2), (42)");
        execute("refresh table doc.tbl");

        PostgresNetty postgresNetty = cluster().getInstance(PostgresNetty.class);
        int port = postgresNetty.boundAddress().publishAddress().getPort();
        String url = "jdbc:postgresql://127.0.0.1:" + port + '/';
        execute(
            "create server pg foreign data wrapper jdbc options (url ?)",
            new Object[] { url }
        );

        execute("select foreign_server_name, foreign_data_wrapper_name from information_schema.foreign_servers");
        assertThat(response).hasRows(
            "pg| jdbc"
        );

        String stmt = """
            CREATE FOREIGN TABLE doc.dummy (x int)
            SERVER pg
            OPTIONS (schema_name 'doc', table_name 'tbl')
            """;
        execute(stmt);

        execute("select foreign_table_schema, foreign_table_name from information_schema.foreign_tables");
        assertThat(response).hasRows(
            "doc| dummy"
        );
        execute("select table_schema, table_name from information_schema.tables where table_type = 'FOREIGN'");
        assertThat(response).hasRows(
            "doc| dummy"
        );
        execute("select column_name from information_schema.columns where table_name = 'dummy' order by 1");
        assertThat(response).hasRows(
            "x"
        );

        execute("grant dql on table doc.tbl to arthur");
        execute("grant dql on table doc.dummy to trillian");

        assertThatThrownBy(() -> execute(stmt))
            .as("Cannot create foreign table with same name again")
            .hasMessageContaining("already exists.");


        execute("create user mapping for trillian server pg options (\"user\" 'arthur', password 'not-so-secret')");
        execute("explain select * from doc.dummy order by x");
        assertThat(response).hasLines(
            "OrderBy[x ASC] (rows=unknown)",
            "  └ ForeignCollect[x] (rows=unknown)"
        );

        var roles = cluster().getInstance(Roles.class);
        Role trillian = roles.findUser("trillian");
        response = sqlExecutor.executeAs("select * from doc.dummy order by x asc", trillian);
        assertThat(response).hasRows(
            "1",
            "2",
            "42"
        );

        response = sqlExecutor.executeAs("select {x=x} from doc.dummy order by x asc", trillian);
        assertThat(response).hasRows(
            "{x=1}",
            "{x=2}",
            "{x=42}"
        );


        // information is persisted and survives restart
        cluster().fullRestart();
        assertBusy(() -> {
            execute("select foreign_server_name, foreign_data_wrapper_name from information_schema.foreign_servers");
            assertThat(response).hasRows(
                "pg| jdbc"
            );
        });
        execute("select foreign_table_schema, foreign_table_name from information_schema.foreign_tables");
        assertThat(response).hasRows(
            "doc| dummy"
        );
        execute("select table_schema, table_name from information_schema.tables where table_type = 'FOREIGN'");
        assertThat(response).hasRows(
            "doc| dummy"
        );
        execute("select column_name from information_schema.columns where table_name = 'dummy' order by 1");
        assertThat(response).hasRows(
            "x"
        );

        assertThatThrownBy(() -> execute("drop server pg"))
            .hasMessageContaining("Cannot drop server `pg` because foreign tables depend on it");

        execute("drop server pg cascade");
        assertThat(execute("select * from information_schema.foreign_servers"))
            .isEmpty();
        assertThat(execute("select foreign_table_schema, foreign_table_name from information_schema.foreign_tables"))
            .isEmpty();

        assertThatThrownBy(() -> execute("drop server pg"))
            .hasMessageContaining("Server `pg` not found");

        execute("drop server if exists pg");
    }

    @Test
    public void test_can_drop_foreign_table() throws Exception {
        PostgresNetty postgresNetty = cluster().getInstance(PostgresNetty.class);
        int port = postgresNetty.boundAddress().publishAddress().getPort();
        String url = "jdbc:postgresql://127.0.0.1:" + port + '/';
        execute(
            "create server pg foreign data wrapper jdbc options (url ?)",
            new Object[] { url }
        );

        String stmt = """
            CREATE FOREIGN TABLE doc.dummy (x int)
            SERVER pg
            OPTIONS (schema_name 'doc', table_name 'tbl')
            """;
        execute(stmt);

        execute("select foreign_table_schema, foreign_table_name from information_schema.foreign_tables");
        assertThat(response).hasRows(
            "doc| dummy"
        );

        assertThatThrownBy(() -> execute("drop foreign table doc.dummy cascade"))
            .hasMessageContaining("DROP FOREIGN TABLE with CASCADE is not supported");

        execute("drop foreign table doc.dummy");
        execute("select foreign_table_schema, foreign_table_name from information_schema.foreign_tables");
        assertThat(response).isEmpty();

        assertThatThrownBy(() -> execute("drop foreign table doc.dummy"))
            .hasMessageContaining("Relation 'doc.dummy' unknown");
        execute("drop foreign table if exists doc.dummy");
    }

    @Test
    public void test_can_drop_user_mapping() throws Exception {
        execute("create user trillian with (password = 'secret')");
        PostgresNetty postgresNetty = cluster().getInstance(PostgresNetty.class);
        int port = postgresNetty.boundAddress().publishAddress().getPort();
        String url = "jdbc:postgresql://127.0.0.1:" + port + '/';
        execute(
            "create server pg foreign data wrapper jdbc options (url ?)",
            new Object[] { url }
        );

        String stmt = """
            CREATE FOREIGN TABLE doc.summits (mountain text, height int)
            SERVER pg
            OPTIONS (schema_name 'sys', table_name 'summits')
            """;
        execute(stmt);

        execute("create user mapping for current_user server pg options (\"user\" 'trillian', password 'secret')");
        String selectQuery = "select mountain from doc.summits order by height desc limit 3";
        assertThatThrownBy(() -> execute(selectQuery))
            .as("Cannot access table with user mapped to trillian")
            .hasMessageContaining("Schema 'sys' unknown");

        execute("drop user mapping for current_user server pg");
        assertThatThrownBy(() -> execute("drop user mapping for current_user server pg"))
            .hasMessageContaining("No user mapping found for user `crate` and server `pg`");
        execute("drop user mapping if exists for current_user server pg");

        execute(selectQuery);
        assertThat(response).hasRows(
            "Mont Blanc",
            "Monte Rosa",
            "Dom"
        );
    }
}
