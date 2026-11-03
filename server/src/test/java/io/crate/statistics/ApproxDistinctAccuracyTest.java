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

package io.crate.statistics;

import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class ApproxDistinctAccuracyTest {

    private static final String URL = "jdbc:postgresql://localhost:5432/doc";
    private static final String USER = "crate";
    private static final String PASSWORD = "mysecretpassword";
    private static final int TABLE_ROWS = 5_000_000;
    private static final int CHUNK_ROWS = 500_000;

    @Test
    public void test_low_skew() throws Exception {
        String table = "approx_distinct";
        int[] percents = {0, 5, 15, 25, 35, 45, 55, 65, 75, 85, 95};
        List<String> columns = new ArrayList<>();
        List<String> definitions = new ArrayList<>();
        List<String> expressions = new ArrayList<>();
        Map<String, Long> actualDistinct = new HashMap<>();
        for (int percent : percents) {
            String suffix = String.format(Locale.ENGLISH, "%02d", percent);
            int distinct = percent == 0 ? 50 : TABLE_ROWS / 100 * percent;
            actualDistinct.put("str_" + suffix, (long) distinct);
            actualDistinct.put("int_" + suffix, (long) distinct);
            columns.add("str_" + suffix);
            definitions.add("str_" + suffix + " TEXT");
            expressions.add("'s' || CAST(generate_series % " + distinct + " AS TEXT)");
            columns.add("int_" + suffix);
            definitions.add("int_" + suffix + " INTEGER");
            expressions.add("generate_series % " + distinct);
        }

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            loadIfMissing(conn, stmt, table, columns, definitions, expressions);
            stmt.execute("ANALYZE");
            printStats(stmt, table, actualDistinct);
        }
    }

    @Test
    public void test_high_skew() throws Exception {
        String table = "approx_distinct_skew";
        double[] exponents = {1.1, 1.5, 2.0};
        int[] domains = {1_000, 100_000, 5_000_000};
        String[] domainLabels = {"1k", "100k", "5m"};
        List<String> columns = new ArrayList<>();
        List<String> definitions = new ArrayList<>();
        List<String> expressions = new ArrayList<>();
        for (double s : exponents) {
            for (int i = 0; i < domains.length; i++) {
                String suffix = String.format(Locale.ENGLISH, "s%02d_%s", Math.round(s * 10), domainLabels[i]);
                // Inverse CDF of a power law with exponent `s` over the ranks `1..D`,
                // so that rank `r` appears about `r^(-s)` times:
                // floor((1 + u * (D^(1-s) - 1))^(1/(1-s))), with u = (g - 0.5) / N
                String rank = String.format(
                    Locale.ENGLISH,
                    "CAST(floor(power(1 + (generate_series - 0.5) * %s, %s)) AS INTEGER)",
                    (Math.pow(domains[i], 1 - s) - 1) / TABLE_ROWS,
                    1 / (1 - s));
                columns.add("str_" + suffix);
                definitions.add("str_" + suffix + " TEXT");
                expressions.add("'s' || CAST(" + rank + " AS TEXT)");
                columns.add("int_" + suffix);
                definitions.add("int_" + suffix + " INTEGER");
                expressions.add(rank);
            }
        }

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            loadIfMissing(conn, stmt, table, columns, definitions, expressions);
            // Many of the rare ranks get no row at all, so the actual distinct count is not `D`.
            Map<String, Long> actualDistinct = new HashMap<>();
            for (String column : columns) {
                try (ResultSet rs = stmt.executeQuery("SELECT count(DISTINCT " + column + ") FROM " + table)) {
                    rs.next();
                    actualDistinct.put(column, rs.getLong(1));
                }
            }
            stmt.execute("ANALYZE");
            printStats(stmt, table, actualDistinct);
        }
    }

    private static void loadIfMissing(Connection conn,
                                      Statement stmt,
                                      String table,
                                      List<String> columns,
                                      List<String> definitions,
                                      List<String> expressions) throws SQLException {
        if (isLoaded(stmt, table)) {
            return;
        }
        stmt.execute("DROP TABLE IF EXISTS " + table);
        stmt.execute("CREATE TABLE " + table + " (" + String.join(", ", definitions) + ")");
        String insertSql = "INSERT INTO " + table + " (" + String.join(", ", columns) + ") "
            + "SELECT " + String.join(", ", expressions) + " FROM generate_series(?, ?)";
        try (PreparedStatement insert = conn.prepareStatement(insertSql)) {
            for (int start = 1; start <= TABLE_ROWS; start += CHUNK_ROWS) {
                insert.setInt(1, start);
                insert.setInt(2, start + CHUNK_ROWS - 1);
                insert.executeUpdate();
            }
        }
        try (ResultSet rs = stmt.executeQuery("SELECT version()")) {
            rs.next();
            if (rs.getString(1).startsWith("CrateDB")) {
                stmt.execute("REFRESH TABLE " + table);
            }
        }
    }

    private static void printStats(Statement stmt, String table, Map<String, Long> actualDistinct) throws SQLException {
        StringBuilder sb = new StringBuilder();
        try (ResultSet rs = stmt.executeQuery("SELECT version()")) {
            rs.next();
            sb.append(String.format(Locale.ENGLISH, "%n%s%n", rs.getString(1)));
        }
        sb.append(String.format(Locale.ENGLISH, "%n%-14s %10s %15s %10s%n", "attname", "actual", "n_distinct", "ratio"));
        try (ResultSet rs = stmt.executeQuery(
            "SELECT attname, n_distinct FROM pg_stats WHERE tablename = '" + table + "' ORDER BY attname")) {
            while (rs.next()) {
                String attname = rs.getString(1);
                float nDistinct = rs.getFloat(2);
                long actual = actualDistinct.get(attname);
                // PostgreSQL reports -(distinct / rows) if it assumes the NDV grows with the table
                double estimate = nDistinct < 0 ? -nDistinct * TABLE_ROWS : nDistinct;
                sb.append(String.format(
                    Locale.ENGLISH,
                    "%-14s %10d %15s %9.2f%%%n",
                    attname,
                    actual,
                    nDistinct,
                    estimate / actual * 100));
            }
        }
        System.out.println(sb);
    }

    private static boolean isLoaded(Statement stmt, String table) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = '" + table + "'")) {
            rs.next();
            if (rs.getLong(1) == 0) {
                return false;
            }
        }
        try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM " + table)) {
            rs.next();
            return rs.getLong(1) == TABLE_ROWS;
        }
    }
}
