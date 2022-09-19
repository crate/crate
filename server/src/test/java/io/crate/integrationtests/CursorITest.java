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
        // Using JDBC to be able to re-use the same connection to the same node
        try (var conn = DriverManager.getConnection(url(), properties)) {
            Statement statement = conn.createStatement();
            conn.setAutoCommit(false);
            String declare = "declare c1 no scroll cursor for select * from generate_series(1, 10)";
            statement.execute(declare);

            ResultSet result = statement.executeQuery("fetch from c1");
            assertThat(result.next()).isEqualTo(true);
            assertThat(result.getInt(1)).isEqualTo(1);
            assertThat(result.next()).isEqualTo(false);

            result = statement.executeQuery("fetch forward 2 from c1");
            assertThat(result.next()).isEqualTo(true);
            assertThat(result.getInt(1)).isEqualTo(2);
            assertThat(result.next()).isEqualTo(true);
            assertThat(result.getInt(1)).isEqualTo(3);

            statement.execute("close c1");

            assertThatThrownBy(() -> statement.executeQuery("fetch forward 2 from c1"))
                .hasMessageContaining("No cursor named `c1` available");
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
            assertThat(result.next()).isEqualTo(true);
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
