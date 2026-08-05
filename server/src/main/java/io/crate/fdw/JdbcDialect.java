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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.logging.log4j.Logger;

enum JdbcDialect {

    POSTGRES {
        @Override
        public ForeignTableStats getStats(String url, Properties properties, String schema, String table, Logger logger) {
            String statsQuery = "SELECT c.reltuples, pg_relation_size(c.oid) FROM pg_class c " +
                "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                "WHERE n.nspname = ? AND c.relname = ?";
            try (Connection conn = DriverManager.getConnection(url, properties);
                 PreparedStatement stmt = conn.prepareStatement(statsQuery)) {

                stmt.setString(1, schema);
                stmt.setString(2, table);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        long numDocs = rs.getLong(1);
                        long sizeInBytes = rs.getLong(2);
                        return new ForeignTableStats(Math.max(0L, numDocs), Math.max(0L, sizeInBytes));
                    }
                }
            } catch (Exception e) {
                logger.debug("Unable to fetch statistics for PostgreSQL foreign table {}.{}", schema, table, e);
            }
            return ForeignTableStats.EMPTY;
        }
    },

    GENERIC {
        @Override
        public ForeignTableStats getStats(String url, Properties properties, String schema, String table, Logger logger) {
            return ForeignTableStats.EMPTY;
        }
    };

    public abstract ForeignTableStats getStats(String url, Properties properties, String schema, String table, Logger logger);

    public static JdbcDialect fromUrl(String url) {
        if (url != null && url.startsWith("jdbc:postgresql:")) {
            return POSTGRES;
        }
        return GENERIC;
    }
}
