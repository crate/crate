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

package io.crate.integrationtests;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.postgresql.util.PSQLException;

import java.sql.*;

import static org.hamcrest.core.Is.is;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class PostgresITest extends SQLTransportIntegrationTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put("network.psql", true)
            .build();
    }

    @Before
    public void initDriver() throws Exception {
        Class.forName("org.postgresql.Driver");
    }

    @Test
    public void testSelectPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:4242/")) {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement("select name from sys.cluster");
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testSelect() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:4242/")) {
            conn.setAutoCommit(true);
            Statement statement = conn.createStatement();
            statement.execute("select name from sys.cluster");
            ResultSet resultSet = statement.getResultSet();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testSelectWithParameters() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:4242/")) {
            conn.setAutoCommit(true);
            PreparedStatement preparedStatement = conn.prepareStatement("select name from sys.cluster where name like ?");
            preparedStatement.setString(1, "SUITE%");
            ResultSet resultSet = preparedStatement.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }

    @Test
    public void testStatementThatResultsInParseError() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:4242/")) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select name fro sys.cluster");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("mismatched input 'sys'");
            stmt.executeQuery();
        }
    }

    @Test
    public void testStatementThatResultsInAnalyzerError() throws Exception {
        try (Connection conn = DriverManager.getConnection("jdbc:postgresql://127.0.0.1:4242/")) {
            conn.setAutoCommit(true);
            PreparedStatement stmt = conn.prepareStatement("select invalid_column from sys.cluster");
            expectedException.expect(PSQLException.class);
            expectedException.expectMessage("Column invalid_column unknown");
            stmt.executeQuery();

            // verify that queries can be made after an error occurred
            stmt = conn.prepareStatement("select invalid_column from sys.cluster");
            ResultSet resultSet = stmt.executeQuery();
            assertThat(resultSet.next(), is(true));
            assertThat(resultSet.getString(1), Matchers.startsWith("SUITE-CHILD"));
        }
    }
}
