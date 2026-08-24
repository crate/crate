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

import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.RelationMetadata;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.statistics.ColumnStats;
import io.crate.statistics.MostCommonValues;
import io.crate.statistics.Stats;

enum JdbcDialect {

    POSTGRES {
        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public Stats getStats(String url, Properties properties, String schema, String table, RelationMetadata.ForeignTable foreignTable, Logger logger) throws Exception {
            String tableStatsQuery = "SELECT c.reltuples, pg_relation_size(c.oid) FROM pg_class c " +
                "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                "WHERE n.nspname = ? AND c.relname = ?";

            String colStatsQuery = "SELECT attname, null_frac, avg_width, n_distinct, " +
                "most_common_vals, most_common_freqs, histogram_bounds " +
                "FROM pg_stats WHERE schemaname = ? AND tablename = ?";

            try (Connection conn = DriverManager.getConnection(url, properties)) {
                long numDocs;
                long sizeInBytes = 0;

                try (PreparedStatement stmt = conn.prepareStatement(tableStatsQuery)) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            numDocs = Math.max(0L, rs.getLong(1));
                            sizeInBytes = Math.max(0L, rs.getLong(2));
                        } else {
                            return Stats.EMPTY;
                        }
                    }
                } catch (SQLException e) {
                    String fallbackQuery = "SELECT c.reltuples FROM pg_class c " +
                        "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                        "WHERE n.nspname = ? AND c.relname = ?";
                    try (PreparedStatement stmt = conn.prepareStatement(fallbackQuery)) {
                        stmt.setString(1, schema);
                        stmt.setString(2, table);
                        try (ResultSet rs = stmt.executeQuery()) {
                            if (rs.next()) {
                                numDocs = Math.max(0L, rs.getLong(1));
                            } else {
                                return Stats.EMPTY;
                            }
                        }
                    }
                }

                Map<ColumnIdent, ColumnStats<?>> columnStats = new HashMap<>();
                try (PreparedStatement stmt = conn.prepareStatement(colStatsQuery)) {
                    stmt.setString(1, schema);
                    stmt.setString(2, table);
                    try (ResultSet rs = stmt.executeQuery()) {
                        while (rs.next()) {
                            String attname = rs.getString(1);
                            ColumnIdent colIdent = ColumnIdent.of(attname);
                            Reference ref = foreignTable.references().get(colIdent);

                            if (ref != null) {
                                double nullFrac = rs.getDouble(2);
                                double avgWidth = rs.getDouble(3);
                                double nDistinct = rs.getDouble(4);
                                double approxDistinct = nDistinct >= 0 ? nDistinct : -nDistinct * numDocs;

                                List<Object> mcvs = List.of();
                                double[] mcfs = new double[0];
                                List<Object> histogram = List.of();

                                try {
                                    Array mcvSqlArray = rs.getArray(5);
                                    Array mcfSqlArray = rs.getArray(6);
                                    Array histSqlArray = rs.getArray(7);

                                    if (mcvSqlArray != null && mcfSqlArray != null) {
                                        Object[] mcvValues = (Object[]) mcvSqlArray.getArray();
                                        Object[] mcfValues = (Object[]) mcfSqlArray.getArray();

                                        if (mcvValues.length == mcfValues.length && mcvValues.length > 0) {
                                            List<Object> tempMcvs = new ArrayList<>(mcvValues.length);
                                            double[] tempMcfs = new double[mcfValues.length];

                                            for (int i = 0; i < mcvValues.length; i++) {
                                                tempMcvs.add(ref.valueType().implicitCast(mcvValues[i]));
                                                tempMcfs[i] = ((Number) mcfValues[i]).doubleValue();
                                            }

                                            mcvs = tempMcvs;
                                            mcfs = tempMcfs;
                                        }
                                    }

                                    if (histSqlArray != null) {
                                        Object[] histValues = (Object[]) histSqlArray.getArray();
                                        if (histValues.length > 0) {
                                            List<Object> tempHist = new ArrayList<>(histValues.length);
                                            for (Object histValue : histValues) {
                                                tempHist.add(ref.valueType().implicitCast(histValue));
                                            }
                                            histogram = tempHist;
                                        }
                                    }
                                } catch (SQLException | ClassCastException e) {
                                    logger.debug("Skipping MCV/histogram stats for column {}. Failed to extract arrays: {}", attname, e.getMessage());
                                }

                                columnStats.put(colIdent, new ColumnStats(
                                    nullFrac,
                                    avgWidth,
                                    approxDistinct,
                                    ref.valueType(),
                                    mcvs.isEmpty() ? MostCommonValues.empty() : new MostCommonValues(mcvs, mcfs),
                                    histogram
                                ));
                            }
                        }
                    }
                }

                return new Stats(numDocs, sizeInBytes, columnStats);
            }
        }
    },

    GENERIC {
        @Override
        public Stats getStats(String url, Properties properties, String schema, String table, RelationMetadata.ForeignTable foreignTable, Logger logger) throws Exception {
            return Stats.EMPTY;
        }
    };

    public abstract Stats getStats(String url, Properties properties, String schema, String table, RelationMetadata.ForeignTable foreignTable, Logger logger) throws Exception;

    public static JdbcDialect fromUrl(String url) {
        if (url.startsWith("jdbc:postgresql:")) {
            return POSTGRES;
        }
        return GENERIC;
    }
}
