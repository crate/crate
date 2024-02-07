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

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.protocols.postgres.PostgresNetty;
import io.crate.role.Role;
import io.crate.role.Roles;

public class ForeignDataWrapperITest extends IntegTestCase {

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
        var response = sqlExecutor.executeAs("select * from doc.dummy order by x asc", trillian);
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
    }
}
