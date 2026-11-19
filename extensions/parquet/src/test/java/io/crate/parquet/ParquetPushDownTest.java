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
 * distributed under the License is distributed ON an "AS IS" BASIS, WITHOUT
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
import java.util.List;

import org.elasticsearch.common.settings.Settings;
import org.junit.Before;
import org.junit.Test;

import io.crate.planner.operators.LogicalPlan;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;

public class ParquetPushDownTest extends CrateDummyClusterServiceUnitTest {
    private SQLExecutor e;

    @Before
    public void setup() throws Exception {
        Settings options = Settings.builder()
                .put("files", List.of(
                        Paths.get(getClass().getResource("/data").toURI())
                                .resolve("yellow_taxi_01.parquet")
                                .toString(),
                        Paths.get(getClass().getResource("/data").toURI())
                                .resolve("yellow_taxi_03.parquet")
                                .toUri()
                                .toString()))
                .build();
        e = SQLExecutor.of(clusterService)
                .addServer("pq", "parquet", "crate", options)
                .addForeignTable("""
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
                        bstr bit, ip_address ip, ip_address_long ip, car_weight float) server pq
                        """);
    }

    private LogicalPlan plan(String stmt) {
        return e.logicalPlan(stmt);
    }

    @Test
    public void test_pushes_down_double() throws Exception {
        String base = "select trip_distance from taxi where trip_distance ";
        LogicalPlan plan_eq = plan(base + "= 2");
        assertThat(plan_eq).hasOperators("ForeignCollect[doc.taxi | [trip_distance] | (trip_distance = 2.0)]");

        LogicalPlan plan_notEq = plan(base + "!= 2.0");
        assertThat(plan_notEq).hasOperators(
            "Filter[(NOT (trip_distance = 2.0))]",
            "  └ ForeignCollect[doc.taxi | [trip_distance] | true]");

        LogicalPlan plan_gt = plan(base + "> 2.0");
        assertThat(plan_gt).hasOperators("ForeignCollect[doc.taxi | [trip_distance] | (trip_distance > 2.0)]");

        LogicalPlan plan_gtEq = plan(base + ">= 2.0");
        assertThat(plan_gtEq).hasOperators("ForeignCollect[doc.taxi | [trip_distance] | (trip_distance >= 2.0)]");

        LogicalPlan plan_lt = plan(base + "< 2.0");
        assertThat(plan_lt).hasOperators("ForeignCollect[doc.taxi | [trip_distance] | (trip_distance < 2.0)]");

        LogicalPlan plan_ltEq = plan(base + "<= 2.0");
        assertThat(plan_ltEq).hasOperators("ForeignCollect[doc.taxi | [trip_distance] | (trip_distance <= 2.0)]");
    }

    @Test
    public void test_pushes_down_boolean() throws Exception {
        String base = "select big_car from taxi where big_car ";
        LogicalPlan plan_eq = plan(base + "= true");
        assertThat(plan_eq).hasOperators("ForeignCollect[doc.taxi | [big_car] | (big_car = true)]");

        LogicalPlan plan_notEq = plan(base + "!= true");
        assertThat(plan_notEq).hasOperators(
            "Filter[(NOT (big_car = true))]",
            "  └ ForeignCollect[doc.taxi | [big_car] | true]");
    }

}
