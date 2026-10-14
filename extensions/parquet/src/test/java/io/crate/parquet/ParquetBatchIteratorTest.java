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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Test;

import dev.hardwood.InputFile;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.Row;
import io.crate.data.testing.BatchIteratorTester;
import io.crate.data.testing.BatchIteratorTester.ResultOrder;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocTableInfo;
import io.crate.metadata.Reference;
import io.crate.parquet.exceptions.IncompatibleSchemaForParquet;
import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.SQLExecutor;
import io.crate.types.BitStringType;
import io.crate.types.DateType;
import io.crate.types.IpType;
import io.crate.types.NumericType;
import io.crate.types.TimestampType;

public class ParquetBatchIteratorTest extends CrateDummyClusterServiceUnitTest {
    private final List<Path> parquetFile = List.of(
            getDataPath("/data").resolve("yellow_taxi_01.parquet"),
            getDataPath("/data").resolve("yellow_taxi_02.parquet"));

    private SQLExecutor e;

    @Before
    public void prepare() throws Exception {
        e = SQLExecutor.of(clusterService)
                .addTable(
                    """
                    create table doc.taxi
                    (trip_distance double, passenger_count bigint,
                    big_car boolean, trip_type text,
                    car_id uuid, car_sensor object,
                    tire_health array(integer), tpep_pickup_datetime timestamp without time zone,
                    taxi_start date, taxi_start_shift timestamp with time zone,
                    steering_wheel_count smallint, total_amount_earned decimal(18,3),
                    bstr bit, ip_address ip, ip_address_long ip, "VendorID" integer, car_weight float)
                    """);
    }

    @Test
    public void test_correctly_implements_batch_iterator_contracts() throws Exception {
        DocTableInfo table = e.resolveTableInfo("doc.taxi");
        Symbol query = e.asSymbol("true");
        List<Reference> columns = List.of(
                table.getReadReference(ColumnIdent.of("car_weight")),
                table.getReadReference(ColumnIdent.of("VendorID")),
                table.getReadReference(ColumnIdent.of("trip_distance")),
                table.getReadReference(ColumnIdent.of("big_car")),
                table.getReadReference(ColumnIdent.of("steering_wheel_count")),
                table.getReadReference(ColumnIdent.of("passenger_count")),
                table.getReadReference(ColumnIdent.of("total_amount_earned")),
                table.getReadReference(ColumnIdent.of("trip_type")),
                table.getReadReference(ColumnIdent.of("taxi_start")),
                table.getReadReference(ColumnIdent.of("tpep_pickup_datetime")),
                table.getReadReference(ColumnIdent.of("taxi_start_shift")),
                table.getReadReference(ColumnIdent.of("car_id")),
                table.getReadReference(ColumnIdent.of("car_sensor")),
                table.getReadReference(ColumnIdent.of("tire_health")),
                table.getReadReference(ColumnIdent.of("bstr")),
                table.getReadReference(ColumnIdent.of("ip_address")),
                table.getReadReference(ColumnIdent.of("ip_address_long"))
        );
        BatchIteratorTester<Object[]> tester = BatchIteratorTester.forRows(
                () -> new ParquetBatchIterator(InputFile.ofPaths(parquetFile), columns, query),
                ResultOrder.EXACT);
        List<Object[]> expectedResult = List.of(
                new Object[] {1024.128f, 2, 0.97, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("733.085"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:54:04Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("3ac37162-0efb-414e-af0c-adbcfd275026"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 1, 0.9, false, (short) 1, 0L, NumericType.INSTANCE.implicitCast("393.277"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:34:04Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("f498589b-5b46-4d23-83db-33168fe106f9"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 1, 1.4, false, (short) 1, 0L, NumericType.INSTANCE.implicitCast("478.570"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:57:06Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("83838b77-b458-4ec9-b181-8acdd7bc1950"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 2, 5.58, true, (short) 1, 4L, NumericType.INSTANCE.implicitCast("15.603"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:15:22Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("387e67ad-9156-4b16-bf8e-f33dbd9fd68d"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 2, 2.16, false, (short) 1, 0L, NumericType.INSTANCE.implicitCast("505.244"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:27:13Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("bd0a8786-6b47-43d9-a6c6-ae5f1dff2b12"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 2, 2.33, false, (short) 1, 2L, NumericType.INSTANCE.implicitCast("782.168"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:47:11Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("f1330796-6c59-4b26-9198-595cf8bac0c2"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 1, 1.3, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("559.905"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:17:54Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("761e3b54-121d-42c9-985e-67c571d6ea8e"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 1, 2.9, false, (short) 1, 0L, NumericType.INSTANCE.implicitCast("792.465"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:34:28Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("90464235-1ac9-4c7c-a1f2-69b9fc852e38"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 2, 5.34, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("445.029"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:34:14Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("b79869a8-40e6-4617-adc2-b1a3c92dee6a"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 2, 1.83, false, (short) 1, 3L, NumericType.INSTANCE.implicitCast("735.267"), "local", DateType.INSTANCE.implicitCast("2020-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:41:07Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:50.496298+02"), UUID.fromString("b3605695-5054-446e-848a-f2ca7d7ef261"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 25)), List.of(1, 1, 1, 1), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 1.54, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("653.083"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:45:15Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("d0e2d050-6c9d-40de-88e4-19887122e800"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 1.79, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("933.206"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:46:05Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("a9312b3e-68cb-4ecd-8a9b-1f3fca9cb970"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 1.24, false, (short) 1, 2L, NumericType.INSTANCE.implicitCast("623.122"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:22:40Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("75ff9c3e-49e7-4526-a4f4-aed3665e40fd"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 2.0, true, (short) 1, 4L, NumericType.INSTANCE.implicitCast("284.454"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:36:04Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("eb3f8d1f-5ea7-4d6c-946f-ae2d7ffa2523"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 0.83, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("737.791"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:51:23Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("9705d165-1888-40fb-83bd-2d3b37076684"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 3.83, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("333.474"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:48:27Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("e2f82263-f81b-4e86-8499-44835a9a6156"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 5.38, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("748.944"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:45:00Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("c7237362-8280-46d6-9110-f47fddfa14de"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 1.22, false, (short) 1, 2L, NumericType.INSTANCE.implicitCast("6.757"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:51:33Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("612240cb-ab6b-4a30-9e38-c351bb3f3499"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 1.69, false, (short) 1, 3L, NumericType.INSTANCE.implicitCast("806.736"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:40:03Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("87c114e3-30a4-415a-bd75-e1ef37e331e5"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) },
                new Object[] {1024.128f, 7, 1.13, false, (short) 1, 1L, NumericType.INSTANCE.implicitCast("300.954"), "local", DateType.INSTANCE.implicitCast("2024-01-01"), TimestampType.INSTANCE_WITHOUT_TZ.implicitCast("2026-01-01T00:36:07Z"), TimestampType.INSTANCE_WITH_TZ.implicitCast("2026-09-15 14:05:54.143503+02"), UUID.fromString("edb3c0b8-1345-43a1-bf98-acdb9f35948a"), Map.ofEntries(Map.entry("engine_temp", 50), Map.entry("outdoor_temp", 0)), List.of(1, 1, 0, 0), BitStringType.INSTANCE_ONE.implicitCast("101010"), IpType.INSTANCE.implicitCast("127.0.0.1"), IpType.INSTANCE.implicitCast(2130706433) });
        tester.verifyResultAndEdgeCaseBehaviour(expectedResult);
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
        assertThatThrownBy(() -> {
            try {
                it.toList(Row::materialize).get(10, TimeUnit.SECONDS);
            } catch (ExecutionException e) {
                throw Exceptions.toRuntimeException(e);
            }
        })
            .isExactlyInstanceOf(IncompatibleSchemaForParquet.class)
            .hasMessage(
                "The requested column `trip_distance` has type `text`, but that is not compatible with the column in the parquet file");
    }
}
