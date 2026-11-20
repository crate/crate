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
    public void test_pushes_down_bigint() throws Exception {
        String base = "select passenger_count from taxi where passenger_count ";
        LogicalPlan plan_eq = plan(base + "= 2");
        assertThat(plan_eq).hasOperators("ForeignCollect[doc.taxi | [passenger_count] | (passenger_count = 2)]");

        LogicalPlan plan_notEq = plan(base + "!= 2");
        assertThat(plan_notEq).hasOperators(
            "Filter[(NOT (passenger_count = 2))]",
            "  └ ForeignCollect[doc.taxi | [passenger_count] | true]");

        LogicalPlan plan_gt = plan(base + "> 2");
        assertThat(plan_gt).hasOperators("ForeignCollect[doc.taxi | [passenger_count] | (passenger_count > 2)]");

        LogicalPlan plan_gtEq = plan(base + ">= 2");
        assertThat(plan_gtEq).hasOperators("ForeignCollect[doc.taxi | [passenger_count] | (passenger_count >= 2)]");

        LogicalPlan plan_lt = plan(base + "< 2");
        assertThat(plan_lt).hasOperators("ForeignCollect[doc.taxi | [passenger_count] | (passenger_count < 2)]");

        LogicalPlan plan_ltEq = plan(base + "<= 2");
        assertThat(plan_ltEq).hasOperators("ForeignCollect[doc.taxi | [passenger_count] | (passenger_count <= 2)]");
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

    @Test
    public void test_pushes_down_text() throws Exception {
        String base = "select trip_type from taxi where trip_type ";
        LogicalPlan plan_eq = plan(base + "= 'local'");
        assertThat(plan_eq).hasOperators("ForeignCollect[doc.taxi | [trip_type] | (trip_type = 'local')]");

        LogicalPlan plan_notEq = plan(base + "!= 'local'");
        assertThat(plan_notEq).hasOperators(
            "Filter[(NOT (trip_type = 'local'))]",
            "  └ ForeignCollect[doc.taxi | [trip_type] | true]");

        LogicalPlan plan_gt = plan(base + "> 'local'");
        assertThat(plan_gt).hasOperators("ForeignCollect[doc.taxi | [trip_type] | (trip_type > 'local')]");

        LogicalPlan plan_gtEq = plan(base + ">= 'local'");
        assertThat(plan_gtEq).hasOperators("ForeignCollect[doc.taxi | [trip_type] | (trip_type >= 'local')]");

        LogicalPlan plan_lt = plan(base + "< 'local'");
        assertThat(plan_lt).hasOperators("ForeignCollect[doc.taxi | [trip_type] | (trip_type < 'local')]");

        LogicalPlan plan_ltEq = plan(base + "<= 'local'");
        assertThat(plan_ltEq).hasOperators("ForeignCollect[doc.taxi | [trip_type] | (trip_type <= 'local')]");
    }

    @Test
    public void test_pushes_down_uuid() throws Exception {
        String base = "select car_id from taxi where car_id ";
        LogicalPlan plan_eq = plan(base + "= '3ac37162-0efb-414e-af0c-adbcfd275026'");
        assertThat(plan_eq).hasOperators("ForeignCollect[doc.taxi | [car_id] | (car_id = '3ac37162-0efb-414e-af0c-adbcfd275026')]");

        LogicalPlan plan_notEq = plan(base + "!= '3ac37162-0efb-414e-af0c-adbcfd275026'");
        assertThat(plan_notEq).hasOperators(
            "Filter[(NOT (car_id = '3ac37162-0efb-414e-af0c-adbcfd275026'))]",
            "  └ ForeignCollect[doc.taxi | [car_id] | true]");

        LogicalPlan plan_gt = plan(base + "> '3ac37162-0efb-414e-af0c-adbcfd275026'");
        assertThat(plan_gt).hasOperators("ForeignCollect[doc.taxi | [car_id] | (car_id > '3ac37162-0efb-414e-af0c-adbcfd275026')]");

        LogicalPlan plan_gtEq = plan(base + ">= '3ac37162-0efb-414e-af0c-adbcfd275026'");
        assertThat(plan_gtEq).hasOperators("ForeignCollect[doc.taxi | [car_id] | (car_id >= '3ac37162-0efb-414e-af0c-adbcfd275026')]");

        LogicalPlan plan_lt = plan(base + "< '3ac37162-0efb-414e-af0c-adbcfd275026'");
        assertThat(plan_lt).hasOperators("ForeignCollect[doc.taxi | [car_id] | (car_id < '3ac37162-0efb-414e-af0c-adbcfd275026')]");

        LogicalPlan plan_ltEq = plan(base + "<= '3ac37162-0efb-414e-af0c-adbcfd275026'");
        assertThat(plan_ltEq).hasOperators("ForeignCollect[doc.taxi | [car_id] | (car_id <= '3ac37162-0efb-414e-af0c-adbcfd275026')]");
    }

    @Test
    public void test_pushes_down_object_subscript() throws Exception {
        String base = "select car_sensor from taxi where car_sensor['engine_temp'] ";
        LogicalPlan plan_eq = plan(base + "= 2");
        assertThat(plan_eq).hasOperators("ForeignCollect[doc.taxi | [car_sensor] | (car_sensor['engine_temp'] = 2)]");

        LogicalPlan plan_notEq = plan(base + "!= car_sensor['engine_temp']");
        assertThat(plan_notEq).hasOperators(
            "Eval[car_sensor]",
            "  └ Filter[(NOT (car_sensor['engine_temp'] = car_sensor['engine_temp']))]",
            "    └ ForeignCollect[doc.taxi | [car_sensor, car_sensor['engine_temp']] | true]");

        LogicalPlan plan_gt = plan(base + "> 2");
        assertThat(plan_gt).hasOperators("ForeignCollect[doc.taxi | [car_sensor] | (car_sensor['engine_temp'] > 2)]");

        LogicalPlan plan_gtEq = plan(base + ">= 2");
        assertThat(plan_gtEq).hasOperators("ForeignCollect[doc.taxi | [car_sensor] | (car_sensor['engine_temp'] >= 2)]");

        LogicalPlan plan_lt = plan(base + "< 2");
        assertThat(plan_lt).hasOperators("ForeignCollect[doc.taxi | [car_sensor] | (car_sensor['engine_temp'] < 2)]");

        LogicalPlan plan_ltEq = plan(base + "<= 2");
        assertThat(plan_ltEq).hasOperators("ForeignCollect[doc.taxi | [car_sensor] | (car_sensor['engine_temp'] <= 2)]");
    }

}
