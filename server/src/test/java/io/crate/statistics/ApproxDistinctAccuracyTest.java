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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.junit.Test;

import io.crate.types.IntegerType;

public class ApproxDistinctAccuracyTest {

    private static final int NUM_ROWS = 2_000_000;
    private static final int NUM_NODES = 3;

    private static final String URL = "jdbc:postgresql://localhost:5433/doc";
    private static final String USER = "crate";
    private static final String PASSWORD = "mysecretpassword";
    private static final int TABLE_ROWS = 5_000_000;
    private static final int CHUNK_ROWS = 500_000;

    @Test
    public void test_create_cardinality_table() throws Exception {
        int[] percents = {0, 5, 15, 25, 35, 45, 55, 65, 75, 85, 95};
        List<String> columns = new ArrayList<>();
        List<String> definitions = new ArrayList<>();
        List<String> expressions = new ArrayList<>();
        for (int percent : percents) {
            String suffix = String.format(Locale.ENGLISH, "%02d", percent);
            int distinct = percent == 0 ? 50 : TABLE_ROWS / 100 * percent;
            columns.add("str_" + suffix);
            definitions.add("str_" + suffix + " TEXT");
            expressions.add("'s' || CAST(generate_series % " + distinct + " AS TEXT)");
            columns.add("int_" + suffix);
            definitions.add("int_" + suffix + " INTEGER");
            expressions.add("generate_series % " + distinct);
        }

        try (Connection conn = DriverManager.getConnection(URL, USER, PASSWORD);
             Statement stmt = conn.createStatement()) {
            if (isLoaded(stmt) == false) {
                stmt.execute("DROP TABLE IF EXISTS approx_distinct");
                stmt.execute("CREATE TABLE approx_distinct (" + String.join(", ", definitions) + ")");
                String insertSql = "INSERT INTO approx_distinct (" + String.join(", ", columns) + ") "
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
                        stmt.execute("REFRESH TABLE approx_distinct");
                    }
                }
            }
            stmt.execute("ANALYZE");
            StringBuilder sb = new StringBuilder();
            sb.append(String.format(Locale.ENGLISH, "%n%-10s %15s%n", "attname", "n_distinct"));
            try (ResultSet rs = stmt.executeQuery(
                "SELECT attname, n_distinct FROM pg_stats WHERE tablename = 'approx_distinct' ORDER BY attname")) {
                while (rs.next()) {
                    sb.append(String.format(Locale.ENGLISH, "%-10s %15s%n", rs.getString(1), rs.getString(2)));
                }
            }
            System.out.println(sb);
        }
    }

    private static boolean isLoaded(Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'approx_distinct'")) {
            rs.next();
            if (rs.getLong(1) == 0) {
                return false;
            }
        }
        try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM approx_distinct")) {
            rs.next();
            return rs.getLong(1) == TABLE_ROWS;
        }
    }

    @Test
    public void test_print_approx_distinct_accuracy() {
        Random random = new Random();
        Map<String, Supplier<int[]>> populations = new LinkedHashMap<>();
        populations.put("unique", () -> IntStream.range(0, NUM_ROWS).toArray());
        populations.put("uniform 1M", () -> uniform(random, 1_000_000));
        populations.put("uniform 100k", () -> uniform(random, 100_000));
        populations.put("zipf 1.0", () -> zipf(random, 1_000_000, 1.0));
        populations.put("zipf 1.5", () -> zipf(random, 1_000_000, 1.5));
        populations.put("low-card 50", () -> uniform(random, 50));

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(Locale.ENGLISH, "%n%-14s %10s %10s %10s %10s %10s %13s%n",
            "population", "true", "today", "exact f1", "sketch", "merged", "merged/true"));
        for (var entry : populations.entrySet()) {
            int[] rows = entry.getValue().get();
            long trueDistinct = countDistinct(rows);
            int[] sample = sample(random, rows, Reservoir.NUM_SAMPLES);

            ColumnSketchBuilder<Integer> single = IntegerType.INSTANCE.columnStatsSupport().sketchBuilder();
            List<ColumnSketchBuilder<Integer>> perNode = new ArrayList<>(NUM_NODES);
            for (int i = 0; i < NUM_NODES; i++) {
                perNode.add(IntegerType.INSTANCE.columnStatsSupport().sketchBuilder());
            }
            for (int i = 0; i < sample.length; i++) {
                single.add(sample[i]);
                perNode.get(i % NUM_NODES).add(sample[i]);
            }
            ColumnSketchBuilder<Integer> merged = perNode.get(0);
            for (int i = 1; i < NUM_NODES; i++) {
                merged = merged.merge(perNode.get(i));
            }

            double today = single.distinctSketch.getEstimate();
            double exact = exactDuj1(sample, rows.length);
            double sketch = single.toStats(rows.length).approxDistinct();
            double mergedEstimate = merged.toStats(rows.length).approxDistinct();
            sb.append(String.format(Locale.ENGLISH, "%-14s %10d %10.0f %10.0f %10.0f %10.0f %13.2f%n",
                entry.getKey(), trueDistinct, today, exact, sketch, mergedEstimate, mergedEstimate / trueDistinct));
        }
        System.out.println(sb);
    }

    private static double exactDuj1(int[] sample, long numRows) {
        Map<Integer, Integer> counts = new HashMap<>();
        for (int value : sample) {
            counts.merge(value, 1, Integer::sum);
        }
        double d = counts.size();
        double f1 = counts.values().stream().filter(c -> c == 1).count();
        return ColumnSketchBuilder.approximateNumDistinct(0.0, d, d - f1, sample.length, numRows);
    }

    private static int[] uniform(Random random, int numValues) {
        int[] rows = new int[NUM_ROWS];
        for (int i = 0; i < NUM_ROWS; i++) {
            rows[i] = random.nextInt(numValues);
        }
        return rows;
    }

    private static int[] zipf(Random random, int numValues, double exponent) {
        double[] cumulative = new double[numValues];
        double sum = 0;
        for (int i = 0; i < numValues; i++) {
            sum += 1.0 / Math.pow(i + 1, exponent);
            cumulative[i] = sum;
        }
        int[] rows = new int[NUM_ROWS];
        for (int i = 0; i < NUM_ROWS; i++) {
            int idx = Arrays.binarySearch(cumulative, random.nextDouble() * sum);
            rows[i] = idx >= 0 ? idx : -idx - 1;
        }
        return rows;
    }

    private static long countDistinct(int[] rows) {
        int[] sorted = rows.clone();
        Arrays.sort(sorted);
        long distinct = sorted.length > 0 ? 1 : 0;
        for (int i = 1; i < sorted.length; i++) {
            if (sorted[i] != sorted[i - 1]) {
                distinct++;
            }
        }
        return distinct;
    }

    private static int[] sample(Random random, int[] rows, int sampleSize) {
        int[] shuffled = rows.clone();
        for (int i = 0; i < sampleSize; i++) {
            int j = i + random.nextInt(shuffled.length - i);
            int tmp = shuffled[i];
            shuffled[i] = shuffled[j];
            shuffled[j] = tmp;
        }
        return Arrays.copyOf(shuffled, sampleSize);
    }
}
