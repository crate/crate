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

import org.junit.Test;

import io.crate.expression.tablefunctions.AbstractTableFunctionsTest;

public class ParquetSchemaTableFuncTest extends AbstractTableFunctionsTest {

    @Test
    public void test_read_schema() throws Exception {
        Path path = getDataPath("/data").resolve("yellow_taxi_01.parquet");
        String expected = """
            VendorID| INT32| NULL| OPTIONAL| INT_32| 0
            tpep_pickup_datetime| INT64| NULL| OPTIONAL| TIMESTAMP(MICROS, local)| 1
            tpep_dropoff_datetime| INT64| NULL| OPTIONAL| TIMESTAMP(MICROS, local)| 2
            passenger_count| INT64| NULL| OPTIONAL| INT_64| 3
            trip_distance| DOUBLE| NULL| OPTIONAL| NULL| 4
            RatecodeID| INT64| NULL| OPTIONAL| INT_64| 5
            store_and_fwd_flag| BYTE_ARRAY| NULL| OPTIONAL| STRING| 6
            PULocationID| INT32| NULL| OPTIONAL| INT_32| 7
            DOLocationID| INT32| NULL| OPTIONAL| INT_32| 8
            payment_type| INT64| NULL| OPTIONAL| INT_64| 9
            fare_amount| DOUBLE| NULL| OPTIONAL| NULL| 10
            extra| DOUBLE| NULL| OPTIONAL| NULL| 11
            mta_tax| DOUBLE| NULL| OPTIONAL| NULL| 12
            tip_amount| DOUBLE| NULL| OPTIONAL| NULL| 13
            tolls_amount| DOUBLE| NULL| OPTIONAL| NULL| 14
            improvement_surcharge| DOUBLE| NULL| OPTIONAL| NULL| 15
            total_amount| DOUBLE| NULL| OPTIONAL| NULL| 16
            congestion_surcharge| DOUBLE| NULL| OPTIONAL| NULL| 17
            Airport_fee| DOUBLE| NULL| OPTIONAL| NULL| 18
            cbd_congestion_fee| DOUBLE| NULL| OPTIONAL| NULL| 19
            big_car| BOOLEAN| NULL| OPTIONAL| NULL| 20
            trip_type| BYTE_ARRAY| NULL| OPTIONAL| STRING| 21
            car_id| FIXED_LEN_BYTE_ARRAY| 16| OPTIONAL| UUID| 22
            element| INT32| NULL| OPTIONAL| INT_32| 23
            taxi_start| INT32| NULL| OPTIONAL| DATE| 24
            taxi_start_shift| INT64| NULL| OPTIONAL| TIMESTAMP(MICROS, UTC)| 25
            steering_wheel_count| INT32| NULL| OPTIONAL| INT_16| 26
            total_amount_earned| INT64| NULL| OPTIONAL| DECIMAL(18, 3)| 27
            bstr| BYTE_ARRAY| NULL| OPTIONAL| STRING| 28
            ip_address| BYTE_ARRAY| NULL| OPTIONAL| STRING| 29
            ip_address_long| INT64| NULL| OPTIONAL| INT_64| 30
            car_weight| FLOAT| NULL| OPTIONAL| NULL| 31
            engine_temp| INT32| NULL| OPTIONAL| INT_32| 32
            unit| BYTE_ARRAY| NULL| OPTIONAL| STRING| 33
            value| INT32| NULL| OPTIONAL| INT_32| 34
            car_sensor_json| BYTE_ARRAY| NULL| OPTIONAL| JSON| 35
            engine_temp| INT32| NULL| OPTIONAL| INT_32| 36
            outdoor_temp| BYTE_ARRAY| NULL| OPTIONAL| JSON| 37
            car_sensor_json_ignored_policy| BYTE_ARRAY| NULL| OPTIONAL| JSON| 38
            """;
        assertExecute("parquet_schema(?)", expected, path.toString());
    }

    @Test
    public void test_parquet_schema_null_argument() throws Exception {
        assertThatThrownBy(() -> assertExecute("parquet_schema(null)", ""))
            .isExactlyInstanceOf(NullPointerException.class)
            .hasMessage("URI argument to parquet_schema must not be null");
    }
}

