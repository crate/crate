/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static io.crate.fdw.JdbcDialect.POSTGRES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.mockito.MockedStatic;

public class JdbcDialectTest {

    @Test
    public void test_from_url_resolves_correct_dialect() {
        assertThat(JdbcDialect.fromUrl("jdbc:postgresql://localhost:5432/db"))
            .isEqualTo(POSTGRES);

        assertThat(JdbcDialect.fromUrl("jdbc:mysql://localhost:3306/db"))
            .isEqualTo(JdbcDialect.GENERIC);

        assertThat(JdbcDialect.fromUrl(null))
            .isEqualTo(JdbcDialect.GENERIC);
    }

    @Test
    public void test_postgres_dialect_fetches_stats() throws Exception {
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class)) {
            Connection conn = mock(Connection.class);
            PreparedStatement stmt = mock(PreparedStatement.class);
            ResultSet rs = mock(ResultSet.class);

            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                .thenReturn(conn);
            when(conn.prepareStatement(anyString())).thenReturn(stmt);
            when(stmt.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(1000L);
            when(rs.getLong(2)).thenReturn(81920L);

            Logger logger = mock(Logger.class);
            ForeignTableStats stats = POSTGRES.getStats("jdbc:postgresql://fake", new Properties(), "schema", "table", logger);

            assertThat(stats.numDocs()).isEqualTo(1000L);
            assertThat(stats.sizeInBytes()).isEqualTo(81920L);
        }
    }

    @Test
    public void test_negative_stats_are_clamped_to_zero() throws Exception {
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class)) {
            Connection conn = mock(Connection.class);
            PreparedStatement stmt = mock(PreparedStatement.class);
            ResultSet rs = mock(ResultSet.class);

            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                .thenReturn(conn);
            when(conn.prepareStatement(anyString())).thenReturn(stmt);
            when(stmt.executeQuery()).thenReturn(rs);
            when(rs.next()).thenReturn(true);
            when(rs.getLong(1)).thenReturn(-5L);
            when(rs.getLong(2)).thenReturn(-100L);

            Logger logger = mock(Logger.class);
            ForeignTableStats stats = POSTGRES.getStats("jdbc:postgresql://fake", new Properties(), "schema", "table", logger);

            assertThat(stats.numDocs()).isEqualTo(0L);
            assertThat(stats.sizeInBytes()).isEqualTo(0L);
        }
    }

    @Test
    public void test_sql_exception_returns_empty_stats() {
        try (MockedStatic<DriverManager> driverManagerMock = mockStatic(DriverManager.class)) {
            driverManagerMock.when(() -> DriverManager.getConnection(anyString(), any(Properties.class)))
                .thenThrow(SQLException.class);

            Logger logger = mock(Logger.class);
            ForeignTableStats stats = POSTGRES.getStats("jdbc:postgresql://fake", new Properties(), "schema", "table", logger);

            assertThat(stats).isEqualTo(ForeignTableStats.EMPTY);
        }
    }
}
