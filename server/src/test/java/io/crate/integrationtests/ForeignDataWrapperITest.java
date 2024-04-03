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

import static io.crate.testing.Asserts.assertSQLError;
import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

import io.crate.protocols.postgres.PGErrorStatus;
import io.crate.protocols.postgres.PostgresNetty;
import io.crate.role.Role;
import io.crate.role.Roles;
import io.netty.handler.codec.http.HttpResponseStatus;

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
    public void test_full_fdw_flow() throws Exception {
        execute("create user trillian with (password = 'secret')");
        execute("create user arthur with (password = 'not-so-secret')");

        execute("create table doc.tbl (x int, y int)");
        execute("insert into doc.tbl (x, y) values (1, 1), (2, 2), (42, 42)");
        execute("refresh table doc.tbl");

        PostgresNetty postgresNetty = cluster().getInstance(PostgresNetty.class);
        int port = postgresNetty.boundAddress().publishAddress().getPort();
        String url = "jdbc:postgresql://127.0.0.1:" + port + '/';
        String createServerStmt = "create server pg foreign data wrapper jdbc options (url ?)";
        execute(createServerStmt, new Object[] { url });
        assertSQLError(() -> execute(createServerStmt, new Object[] { url }))
            .hasPGError(PGErrorStatus.DUPLICATE_OBJECT)
            .hasHTTPError(HttpResponseStatus.CONFLICT, 4100);

        execute("select foreign_server_name, foreign_data_wrapper_name from information_schema.foreign_servers");
        assertThat(response).hasRows(
            "pg| jdbc"
        );
        execute(
            """
            SELECT
                foreign_server_name,
                option_name,
                option_value
            FROM
                information_schema.foreign_server_options
            ORDER BY
                option_name DESC
            """);
        assertThat(response).hasRows(new Object[] { "pg", "url", url });

        String stmt = """
            CREATE FOREIGN TABLE doc.dummy (x int, y int)
            SERVER pg
            OPTIONS (schema_name 'doc', table_name 'tbl')
            """;
        execute(stmt);

        execute("select foreign_table_schema, foreign_table_name from information_schema.foreign_tables");
        assertThat(response).hasRows(
            "doc| dummy"
        );
        execute(
            """
            SELECT
                foreign_table_schema,
                foreign_table_name,
                option_name,
                option_value
            FROM
                information_schema.foreign_table_options
            ORDER BY
                option_name DESC
            """);
        assertThat(response).hasRows(
            "doc| dummy| table_name| tbl",
            "doc| dummy| schema_name| doc"
        );


        execute("select table_schema, table_name from information_schema.tables where table_type = 'FOREIGN'");
        assertThat(response).hasRows(
            "doc| dummy"
        );
        execute("select column_name from information_schema.columns where table_name = 'dummy' order by 1");
        assertThat(response).hasRows(
            "x",
            "y"
        );

        execute("grant dql on table doc.tbl to arthur");
        execute("grant dql on table doc.dummy to trillian");

        assertThatThrownBy(() -> execute(stmt))
            .as("Cannot create foreign table with same name again")
            .hasMessageContaining("already exists.");


        String createUserMappingStmt =
            "create user mapping for trillian server pg options (\"user\" 'arthur', password 'not-so-secret')";
        execute(createUserMappingStmt);
        execute("select authorization_identifier, foreign_server_name from information_schema.user_mappings");
        assertThat(response).hasRows(
            "trillian| pg"
        );
        assertSQLError(() -> execute(createUserMappingStmt))
            .hasPGError(PGErrorStatus.DUPLICATE_OBJECT)
            .hasHTTPError(HttpResponseStatus.CONFLICT, 4100)
            .hasMessageContaining("USER MAPPING for 'trillian' and server 'pg' already exists");

        execute("explain select * from doc.dummy order by x");
        assertThat(response).hasLines(
            "OrderBy[x ASC] (rows=unknown)",
            "  └ ForeignCollect[doc.dummy | [y, x] | true] (rows=unknown)"
        );

        var roles = cluster().getInstance(Roles.class);
        Role trillian = roles.getUser("trillian");
        response = sqlExecutor.executeAs("select * from doc.dummy order by x asc", trillian);
        assertThat(response).hasRows(
            "1| 1",
            "2| 2",
            "42| 42"
        );

        response = sqlExecutor.executeAs("explain select x from doc.dummy where x > 1 order by x asc", trillian);
        assertThat(response).hasLines(
            "OrderBy[x ASC] (rows=unknown)",
            "  └ ForeignCollect[doc.dummy | [x] | (x > 1)] (rows=unknown)"
        );
        response = sqlExecutor.executeAs("select {x=x} from doc.dummy where x > 1 order by x asc", trillian);
        assertThat(response).hasRows(
            "{x=2}",
            "{x=42}"
        );
        response = sqlExecutor.executeAs("explain select x from doc.dummy where sqrt(y) < 5 order by x asc", trillian);
        assertThat(response).hasLines(
            "Eval[x] (rows=0)",
            "  └ OrderBy[x ASC] (rows=0)",
            "    └ Filter[(sqrt(y) < 5.0)] (rows=0)",
            "      └ ForeignCollect[doc.dummy | [x, y] | true] (rows=unknown)"
        );
        response = sqlExecutor.executeAs("select x from doc.dummy where sqrt(y) < 5 order by x asc", trillian);
        assertThat(response).hasRows(
            "1",
            "2"
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
            "x",
            "y"
        );
        execute("select authorization_identifier, foreign_server_name from information_schema.user_mappings");
        assertThat(response).hasRows(
            "trillian| pg"
        );

        assertThatThrownBy(() -> execute("drop server pg"))
            .hasMessageContaining("Cannot drop server `pg` because foreign tables depend on it");

        execute("drop server pg cascade");
        assertThat(execute("select * from information_schema.foreign_servers"))
            .isEmpty();
        assertThat(execute("select foreign_table_schema, foreign_table_name from information_schema.foreign_tables"))
            .isEmpty();
        execute("select * from information_schema.user_mappings");
        assertThat(response).isEmpty();

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

    @Test
    public void test_can_use_joins_on_foreign_tables() throws Exception {
        PostgresNetty postgresNetty = cluster().getInstance(PostgresNetty.class);
        int port = postgresNetty.boundAddress().publishAddress().getPort();
        String url = "jdbc:postgresql://127.0.0.1:" + port + '/';
        String createServerStmt = "create server pg foreign data wrapper jdbc options (url ?)";
        execute(createServerStmt, new Object[] { url });

        String stmt = """
            CREATE FOREIGN TABLE doc.summits (mountain text, height int)
            SERVER pg
            OPTIONS (schema_name 'sys', table_name 'summits')
            """;
        execute(stmt);

        execute(
            """
            SELECT
                f_summits.mountain,
                sys_summits.country
            FROM
                doc.summits f_summits
                INNER JOIN sys.summits sys_summits ON f_summits.mountain = sys_summits.mountain
            ORDER BY
                f_summits.height DESC
            LIMIT 3
            """
        );
        assertThat(response).hasRows(
            "Mont Blanc| FR/IT",
            "Monte Rosa| CH",
            "Dom| CH"
        );
    }

    @Test
    public void test_mask_foreign_password_from_user_mapping_options_table() {
        execute("create user trillian with (password = 'user1pw')");
        execute("create user arthur with (password = 'user2pw')");
        execute("grant al to trillian");
        execute("grant al to arthur");

        var roles = cluster().getInstance(Roles.class);
        Role trillian = roles.getUser("trillian");
        sqlExecutor.executeAs("""
            CREATE SERVER pg
            FOREIGN DATA WRAPPER jdbc
            OPTIONS (url 'jdbc:postgresql://example.com:5432/');
            """, trillian);
        sqlExecutor.executeAs("""
            CREATE FOREIGN TABLE doc.remote_documents (name text)
            SERVER pg
            OPTIONS (schema_name 'public', table_name 'documents');
            """, trillian);
        sqlExecutor.executeAs("""
            CREATE USER MAPPING FOR trillian
            SERVER pg
            OPTIONS ("user" 'foreign-user', password 'foreign-pw');
            """, trillian);

        // trillian can see the pw because trillian is being mapped
        var response = sqlExecutor.executeAs("select * from information_schema.user_mapping_options where option_name = 'password'", trillian);
        assertThat(response).hasRows("trillian| crate| pg| password| foreign-pw");

        // arthur cannot see the pw because arthur is not being mapped nor is a superuser
        response = sqlExecutor.executeAs("select * from information_schema.user_mapping_options where option_name = 'password'",
            roles.getUser("arthur"));
        assertThat(response).hasRows("trillian| crate| pg| password| NULL");

        // superuser can see the pw
        response = execute("select * from information_schema.user_mapping_options where option_name = 'password'");
        assertThat(response).hasRows("trillian| crate| pg| password| foreign-pw");

        execute("drop user mapping for trillian server pg");
        assertThat(execute("select * from information_schema.user_mapping_options where option_name = 'password'")).isEmpty();
    }
}
