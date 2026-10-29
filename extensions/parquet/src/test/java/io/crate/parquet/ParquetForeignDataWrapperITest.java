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

package io.crate.parquet;

import static io.crate.testing.Asserts.assertThat;

import java.nio.file.Paths;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Test;

public class ParquetForeignDataWrapperITest extends IntegTestCase {

    @After
    public void removeServers() throws Exception {
        execute("drop server if exists pq cascade");
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("fdw.allow_local", true)
                .build();
    }

    @Test
    public void test_can_use_joins_on_foreign_tables() throws Exception {
        String path = Paths.get(getClass().getResource("/data").toURI()).resolve("yellow_taxi_01.parquet").toString();
        String createServerStmt = "create server pq foreign data wrapper parquet options (local_files ?)";
        execute(createServerStmt, new Object[] { path });

        String stmt = """
                CREATE FOREIGN TABLE doc.taxi (car_id uuid)
                SERVER pq
                """;
        execute(stmt);

        execute("create table trips (car_id uuid, passenger_count int)");
        execute("insert into trips (car_id, passenger_count) values ('3ac37162-0efb-414e-af0c-adbcfd275026', 10), ('bd0a8786-6b47-43d9-a6c6-ae5f1dff2b12', 20)");
        execute("refresh table trips");
        execute(
                """
                        SELECT
                            f_taxi.car_id,
                            trips.passenger_count
                        FROM
                            doc.taxi f_taxi
                            INNER JOIN trips ON f_taxi.car_id = trips.car_id
                        ORDER BY
                            trips.passenger_count
                        """);
        assertThat(response).hasRows(
                "3ac37162-0efb-414e-af0c-adbcfd275026| 10",
                "bd0a8786-6b47-43d9-a6c6-ae5f1dff2b12| 20");
    }

}
