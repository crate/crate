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

package io.crate.window;

import static io.crate.testing.Asserts.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

public class WindowFunctionsITest extends IntegTestCase {

    @Test
    public void test_window_function_use_mixed_with_joins() throws Exception {
        execute(
            "CREATE TABLE t.tracking (" +
            "   ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, " +
            "   cluster_id TEXT NOT NULL " +
            ") " +
            "CLUSTERED INTO 1 SHARDS " +
            "WITH (" +
            "   \"column_policy\" = 'strict'," +
            "   \"number_of_replicas\" = '0-all'," +
            "   \"write.wait_for_active_shards\" = '1'" +
            ")"
        );
        execute(
            "CREATE TABLE t.clusters ( " +
            "   id TEXT, " +
            "   project_id TEXT, " +
            "   PRIMARY KEY (id) " +
            ") " +
            "CLUSTERED INTO 1 SHARDS " +
            "WITH ( " +
            "   \"column_policy\" = 'strict', " +
            "   \"number_of_replicas\" = '0-all', " +
            "   \"write.wait_for_active_shards\" = '1' " +
            ")"
        );
        execute(
            "CREATE TABLE t.projects ( " +
            "   id TEXT, " +
            "   organization_id TEXT, " +
            "   PRIMARY KEY (id) " +
            ") " +
            "CLUSTERED INTO 1 SHARDS " +
            "WITH ( " +
            "   \"column_policy\" = 'strict', " +
            "   \"number_of_replicas\" = '0-all', " +
            "   \"write.wait_for_active_shards\" = '1' " +
            ")"
        );
        waitNoPendingTasksOnAll();

        // This led to a "ScopedSymbol cannot be streamed error"
        execute(
            "SELECT " +
            "  date_trunc('day', ts) AS \"time\", " +
            "  SUM(ts - prev_ts) " +
            "FROM " +
            "  ( " +
            "    SELECT " +
            "      t.ts, " +
            "      lag(t.ts) OVER (PARTITION BY t.cluster_id ORDER BY t.ts) AS prev_ts " +
            "    FROM " +
            "      t.tracking AS t " +
            "    INNER JOIN " +
            "      t.clusters AS c ON t.cluster_id = c.id " +
            "    INNER JOIN " +
            "      t.projects AS p ON c.project_id = p.id " +
            "    WHERE " +
            "      t.ts >= '2020-06-07T15:33:42.965Z' " +
            "      AND t.ts <= date_trunc('day', '2020-07-07T15:33:42.965Z') " +
            "      AND p.organization_id NOT IN ('a', 'b') " +
            "  ) AS t1 " +
            "GROUP BY \"time\" " +
            "ORDER BY \"time\""
        );
    }

    @Test
    public void test_window_function_partitioned_on_object_subscript() throws Exception {
        execute(
            """
            CREATE TABLE tbl (
               "x" int not null,
               "data" OBJECT(STRICT) AS (
                  "id" BIGINT,
                  "attribute" TEXT
               )
            )
            CLUSTERED INTO 6 SHARDS
            WITH (
               column_policy = 'strict',
               number_of_replicas = '0-1'
            );
            """
        );
        execute(
            """
            INSERT INTO tbl (x, data) VALUES
            (1, '{"attribute":"a-1","id":1842}'),
            (2, '{"attribute":"a-1","id":1844}'),
            (3, '{"attribute":"a-1","id":1848}'),
            (4, '{"attribute":"a-1","id":1851}'),
            (5, '{"attribute":"a-1","id":1859}'),
            (6, '{"attribute":"a-1","id":1871}'),
            (7, '{"attribute":"a-1","id":1838}');
            """
        );
        execute("refresh table tbl");
        execute(
            """
            CREATE OR REPLACE VIEW v AS
            SELECT x, data FROM tbl
            """
        );
        execute(
            """
            SELECT
                x,
                data['id'],
                data['attribute'],
                rank() OVER (PARTITION BY data['attribute'] ORDER BY x) AS lag_id,
                LEAD(data['id']) OVER (PARTITION BY data['attribute'] ORDER BY x) AS lag_id
            FROM v
            ORDER BY x DESC
            LIMIT 100;
            """
        );
        assertThat(response).hasRows(
            "7| 1838| a-1| 7| NULL",
            "6| 1871| a-1| 6| 1838",
            "5| 1859| a-1| 5| 1871",
            "4| 1851| a-1| 4| 1859",
            "3| 1848| a-1| 3| 1851",
            "2| 1844| a-1| 2| 1848",
            "1| 1842| a-1| 1| 1844"
        );
    }
}
