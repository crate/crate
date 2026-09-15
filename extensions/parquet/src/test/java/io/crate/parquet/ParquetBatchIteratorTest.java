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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import dev.hardwood.InputFile;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchIteratorTester.ResultOrder;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.Reference;
import io.crate.parquet.exceptions.IncompatibleSchemaForParquetException;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.BitStringType;
import io.crate.types.DateType;
import io.crate.types.IpType;
import io.crate.types.NumericType;
import io.crate.types.TimestampType;

public class ParquetBatchIteratorTest extends CrateDummyClusterServiceUnitTest {
    private final List<Path> parquetFile = List.of(
            Paths.get(getClass().getResource("/data").toURI())
                    .resolve("yellow_taxi_01.parquet"),
            Paths.get(getClass().getResource("/data").toURI())
                    .resolve("yellow_taxi_02.parquet"));

    public ParquetBatchIteratorTest() throws URISyntaxException {
    }

    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
                .addTable(
                        "create table doc.taxi" +
                                "(trip_distance double, passenger_count bigint," +
                                "big_car boolean, trip_type text," +
                                "car_id uuid, car_sensor object," +
                                "tire_health array(integer), tpep_pickup_datetime timestamp without time zone," +
                                "taxi_start date, taxi_start_shift timestamp with time zone," +
                                "steering_wheel_count smallint, total_amount_earned decimal(18,3)," +
                                "bstr bit, ip_address ip)");
    }

    @Test
    public void test_reads_double_and_correctly_implements_batch_iterator_contracts() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("trip_distance")));
        BatchIteratorTester<Object[]> tester = BatchIteratorTester.forRows(
                () -> new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query),
                ResultOrder.EXACT);
        List<Object[]> expectedResult = List.of(
                new Object[] { 0.97 },
                new Object[] { 0.9 },
                new Object[] { 1.4 },
                new Object[] { 5.58 },
                new Object[] { 2.16 },
                new Object[] { 2.33 },
                new Object[] { 1.3 },
                new Object[] { 2.9 },
                new Object[] { 5.34 },
                new Object[] { 1.83 },
                new Object[] { 1.54 },
                new Object[] { 1.79 },
                new Object[] { 1.24 },
                new Object[] { 2.0 },
                new Object[] { 0.83 },
                new Object[] { 3.83 },
                new Object[] { 5.38 },
                new Object[] { 1.22 },
                new Object[] { 1.69 },
                new Object[] { 1.13 });
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
    }

    @Test
    public void test_reads_boolean() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(table.getReadReference(ColumnIdent.of("big_car")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        // we should only see the rows for passenger_count and not trip_distance
        assertThat(rows).containsExactly(
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { true },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { true },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false },
                new Object[] { false });
    }

    @Test
    public void test_reads_smallint() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("steering_wheel_count")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 },
                new Object[] { (short) 1 });
    }

    @Test
    public void test_reads_int() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("passenger_count")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { 1L },
                new Object[] { 0L },
                new Object[] { 0L },
                new Object[] { 4L },
                new Object[] { 0L },
                new Object[] { 2L },
                new Object[] { 1L },
                new Object[] { 0L },
                new Object[] { 1L },
                new Object[] { 3L },
                new Object[] { 1L },
                new Object[] { 1L },
                new Object[] { 2L },
                new Object[] { 4L },
                new Object[] { 1L },
                new Object[] { 1L },
                new Object[] { 1L },
                new Object[] { 2L },
                new Object[] { 3L },
                new Object[] { 1L });
    }

    @Test
    public void test_reads_numeric() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("total_amount_earned")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { NumericType.INSTANCE.implicitCast("733.085") },
                new Object[] { NumericType.INSTANCE.implicitCast("393.277") },
                new Object[] { NumericType.INSTANCE.implicitCast("478.570") },
                new Object[] { NumericType.INSTANCE.implicitCast("15.603") },
                new Object[] { NumericType.INSTANCE.implicitCast("505.244") },
                new Object[] { NumericType.INSTANCE.implicitCast("782.168") },
                new Object[] { NumericType.INSTANCE.implicitCast("559.905") },
                new Object[] { NumericType.INSTANCE.implicitCast("792.465") },
                new Object[] { NumericType.INSTANCE.implicitCast("445.029") },
                new Object[] { NumericType.INSTANCE.implicitCast("735.267") },
                new Object[] { NumericType.INSTANCE.implicitCast("653.083") },
                new Object[] { NumericType.INSTANCE.implicitCast("933.206") },
                new Object[] { NumericType.INSTANCE.implicitCast("623.122") },
                new Object[] { NumericType.INSTANCE.implicitCast("284.454") },
                new Object[] { NumericType.INSTANCE.implicitCast("737.791") },
                new Object[] { NumericType.INSTANCE.implicitCast("333.474") },
                new Object[] { NumericType.INSTANCE.implicitCast("748.944") },
                new Object[] { NumericType.INSTANCE.implicitCast("6.757") },
                new Object[] { NumericType.INSTANCE.implicitCast("806.736") },
                new Object[] { NumericType.INSTANCE.implicitCast("300.954") });
    }

    @Test
    public void test_reads_string() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("trip_type")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" },
                new Object[] { "local" });
    }

    @Test
    public void test_reads_date() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("taxi_start")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2020-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") },
                new Object[] { DateType.INSTANCE.implicitCast("2024-01-01") });
    }

    @Test
    public void test_reads_timestamp_without_time_zone() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("tpep_pickup_datetime")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:54:04Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:34:04Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:57:06Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:15:22Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:27:13Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:47:11Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:17:54Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:34:28Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:34:14Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:41:07Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:45:15Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:46:05Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:22:40Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:36:04Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:51:23Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:48:27Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:45:00Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:51:33Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:40:03Z") },
                new Object[] { TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:36:07Z") });
    }

    @Test
    public void test_reads_timestamp_with_time_zone() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("taxi_start_shift")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") },
                new Object[] { TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02") });
    }

    @Test
    public void test_reads_uuid() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("car_id")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { UUID.fromString("3ac37162-0efb-414e-af0c-adbcfd275026") },
                new Object[] { UUID.fromString("f498589b-5b46-4d23-83db-33168fe106f9") },
                new Object[] { UUID.fromString("83838b77-b458-4ec9-b181-8acdd7bc1950") },
                new Object[] { UUID.fromString("387e67ad-9156-4b16-bf8e-f33dbd9fd68d") },
                new Object[] { UUID.fromString("bd0a8786-6b47-43d9-a6c6-ae5f1dff2b12") },
                new Object[] { UUID.fromString("f1330796-6c59-4b26-9198-595cf8bac0c2") },
                new Object[] { UUID.fromString("761e3b54-121d-42c9-985e-67c571d6ea8e") },
                new Object[] { UUID.fromString("90464235-1ac9-4c7c-a1f2-69b9fc852e38") },
                new Object[] { UUID.fromString("b79869a8-40e6-4617-adc2-b1a3c92dee6a") },
                new Object[] { UUID.fromString("b3605695-5054-446e-848a-f2ca7d7ef261") },
                new Object[] { UUID.fromString("d0e2d050-6c9d-40de-88e4-19887122e800") },
                new Object[] { UUID.fromString("a9312b3e-68cb-4ecd-8a9b-1f3fca9cb970") },
                new Object[] { UUID.fromString("75ff9c3e-49e7-4526-a4f4-aed3665e40fd") },
                new Object[] { UUID.fromString("eb3f8d1f-5ea7-4d6c-946f-ae2d7ffa2523") },
                new Object[] { UUID.fromString("9705d165-1888-40fb-83bd-2d3b37076684") },
                new Object[] { UUID.fromString("e2f82263-f81b-4e86-8499-44835a9a6156") },
                new Object[] { UUID.fromString("c7237362-8280-46d6-9110-f47fddfa14de") },
                new Object[] { UUID.fromString("612240cb-ab6b-4a30-9e38-c351bb3f3499") },
                new Object[] { UUID.fromString("87c114e3-30a4-415a-bd75-e1ef37e331e5") },
                new Object[] { UUID.fromString("edb3c0b8-1345-43a1-bf98-acdb9f35948a") });
    }

    @Test
    public void test_reads_object() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("car_sensor")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) },
                new Object[] { Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)) });
    }

    @Test
    public void test_reads_array() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("tire_health")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 1, 1) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) },
                new Object[] { List.of(1, 1, 0, 0) });
    }

    @Test
    public void test_reads_bit() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("bstr")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") },
                new Object[] { BitStringType.INSTANCE_ONE.implicitCast("101010") });
    }

    @Test
    public void test_reads_ip() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        // no predicate
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("ip_address")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        List<Object[]> rows = Utils.getRows(it);
        assertThat(rows).containsExactly(
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") },
                new Object[] { IpType.INSTANCE.implicitCast("127.0.0.1") });
    }

    @Test
    public void test_raises_if_cratedb_foreign_table_schema_is_incompatible_with_the_underlying_parquet_schema()
            throws Exception {
        SQLExecutor executor = SQLExecutor.of(clusterService)
                .addTable("create table doc.taxi_wrong_data_type (trip_distance text)");
        DocTableInfo table = executor.resolveTableInfo("doc.taxi_wrong_data_type");

        Symbol query = executor.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("trip_distance")));
        ParquetBatchIterator it = new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query);
        assertThatThrownBy(() -> Utils.getRows(it))
                .isExactlyInstanceOf(IncompatibleSchemaForParquetException.class)
                .hasMessage(
                        "The requested column `trip_distance` has type `text`, but that cannot be converted to the type of the column in the parquet file");
    }
}
