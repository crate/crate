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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Before;
import org.junit.Test;

import io.crate.protocols.postgres.PostgresNetty;

public class CursorITest extends IntegTestCase {

    private Properties properties;

    private String url() {
        // Tests use JDBC to be able to re-use the same connection to the same node
        PostgresNetty postgresNetty = internalCluster().getInstance(PostgresNetty.class);
        int port = postgresNetty.boundAddress().publishAddress().getPort();
        return "jdbc:postgresql://127.0.0.1:" + port + '/';
    }

    @Before
    public void setup() {
        properties = new Properties();
        properties.setProperty("user", "crate");
    }

    @Test
    public void test_declare_fetch_and_close() throws Exception {
        try (var conn = DriverManager.getConnection(url(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);
            String declare = "declare c1 no scroll cursor for select * from generate_series(1, 10)";
            statement.execute(declare);

            ResultSet result = statement.executeQuery("fetch from c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("fetch forward 2 from c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(2);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(3);

            statement.execute("close c1");

            assertThatThrownBy(() -> statement.executeQuery("fetch forward 2 from c1"))
                .hasMessageContaining("No cursor named `c1` available");
        }
    }

    @Test
    public void test_fetching_from_cursor_positioned_at_end_returns_empty_result() throws Exception {
        try (var conn = DriverManager.getConnection(url(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);

            String declare = "declare c1 cursor for select * from generate_series(1, 10)";
            statement.execute(declare);
            ResultSet result = statement.executeQuery("FETCH ALL FROM c1");
            int nextExpectedResult = 1;
            while (result.next()) {
                assertThat(result.getInt(1)).isEqualTo(nextExpectedResult);
                nextExpectedResult++;
            }
            assertThat(nextExpectedResult)
                .as("FETCH ALL must return all 10 rows")
                .isEqualTo(11);

            result = statement.executeQuery("FETCH FROM c1");
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH FROM c1");
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH backward 2 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(10);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(9);
            assertThat(result.next()).isFalse();
        }
    }

    @Test
    public void test_fetch_0_returns_same_row_again_except_before_start_or_after_end() throws Exception {
        try (var conn = DriverManager.getConnection(url(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);

            String declare = "declare c1 scroll cursor for select * from generate_series(1, 10)";
            statement.execute(declare);

            ResultSet result = statement.executeQuery("FETCH 0 FROM c1");
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH 0 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
            assertThat(result.next()).isFalse();

            statement.executeQuery("FETCH ALL FROM c1");
            result = statement.executeQuery("FETCH 0 FROM c1");
            assertThat(result.next()).isFalse();
        }
    }

    @Test
    public void test_scroll_operations() throws Exception {
        try (var conn = DriverManager.getConnection(url(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);
            statement.execute("CREATE TABLE t1 AS SELECT * FROM generate_series(1,10,1)");
            statement.execute("REFRESH TABLE t1");

            statement.execute("DECLARE c1 SCROLL CURSOR FOR SELECT * FROM t1 ORDER BY 1");
            ResultSet result = statement.executeQuery("FETCH 2 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(2);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH 2 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(3);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(4);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH ABSOLUTE 7 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(7);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH BACKWARD 2 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(6);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(5);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH BACKWARD 0 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(5);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH ALL FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(6);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(7);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(8);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(9);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(10);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH ABSOLUTE 6 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(6);
            assertThat(result.next()).isFalse();
            result = statement.executeQuery("FETCH backward 2 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(5);
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(4);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH RELATIVE -2 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(2);
            assertThat(result.next()).isFalse();

            result = statement.executeQuery("FETCH RELATIVE 7 FROM c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(9);
            assertThat(result.next()).isFalse();
        }
    }

    @Test
    public void test_declare_with_hold_survives_transaction() throws Exception {
        try (var conn = DriverManager.getConnection(url(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);

            String declare = "declare c1 no scroll cursor with hold for select * from generate_series(1, 10)";
            statement.execute(declare);

            conn.commit();

            ResultSet result = statement.executeQuery("fetch from c1");
            assertThat(result.next()).isTrue();
            assertThat(result.getInt(1)).isEqualTo(1);
        }
    }

    @Test
    public void test_declare_without_hold_does_not_survive_transaction() throws Exception {
        try (var conn = DriverManager.getConnection(url(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);

            String declare = "declare c1 no scroll cursor without hold for select * from generate_series(1, 10)";
            statement.execute(declare);

            conn.commit();

            assertThatThrownBy(() -> statement.executeQuery("fetch from c1"))
                .hasMessageContaining("No cursor named `c1` available");
        }
    }

}
