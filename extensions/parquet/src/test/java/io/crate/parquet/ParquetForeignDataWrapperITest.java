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
import org.junit.Before;
import org.junit.Test;

public class ParquetForeignDataWrapperITest extends IntegTestCase {

    @After
    public void removeServers() throws Exception {
        execute("drop server if exists pq cascade");
    }

    @Before
    public void prepare() throws Exception {
        String path = Paths.get(getClass().getResource("/data").toURI()).resolve("yellow_taxi_01.parquet").toString();
        String createServerStmt = "create server pq foreign data wrapper parquet options (files ?)";
        execute(createServerStmt, new Object[] { path });
        String stmt = """
                create foreign table doc.taxi
                (trip_distance double, passenger_count bigint,
                big_car boolean, trip_type text,
                car_id uuid, car_sensor_json object,
                car_sensor_json_ignored_policy object(ignored),
                car_sensor object(strict) as (
                    engine_temp int,
                    outdoor_temp object(strict) as (
                        unit text,
                        value int
                    )
                ),
                car_sensor_mixed_objects object(strict) as (
                    engine_temp int,
                    outdoor_temp object
                ),
                tire_health array(integer), tpep_pickup_datetime timestamp without time zone,
                taxi_start date, taxi_start_shift timestamp with time zone,
                steering_wheel_count smallint, total_amount_earned decimal(18,3),
                bstr bit, ip_address ip, ip_address_long ip, car_weight float) server pq;
                """;
        execute(stmt);
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

    @Test
    public void test_simple_and_filter_bigint() throws Exception {
        String query = "select car_id, passenger_count from doc.taxi where passenger_count > 3 and passenger_count < 5";
        execute(query);
        assertThat(response).hasRows("387e67ad-9156-4b16-bf8e-f33dbd9fd68d| 4");
    }

    @Test
    public void test_simple_and_filter_int() throws Exception {
        String query = "select car_sensor from doc.taxi where car_sensor['engine_temp'] != 50";
        execute(query);
        assertThat(response).isEmpty();
    }

    @Test
    public void test_mixed_and_or_filter() throws Exception {
        String query = "select car_id, passenger_count from doc.taxi where passenger_count = 10 or passenger_count = 4";
        execute(query);
        assertThat(response).hasRows("387e67ad-9156-4b16-bf8e-f33dbd9fd68d| 4");
    }
}
