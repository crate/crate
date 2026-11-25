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

package io.crate.expression.tablefunctions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.stream.StreamSupport;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import io.crate.data.Row;

public class GenerateSeriesTest extends AbstractTableFunctionsTest {

    @Test
    public void testFrom3To4WithDefaultStep() {
        assertExecute(
            "generate_series(3, 4)",
            "3\n" +
            "4\n"
        );
    }

    @Test
    public void test_generate_series_can_be_addressed_via_pg_catalog_schema() {
        assertExecute(
            "pg_catalog.generate_series(3, 4)",
            "3\n" +
            "4\n"
        );
    }

    @Test
    public void from2To8With2AsStep() {
        assertExecute(
            "generate_series(2, 8, 2)",
            "2\n" +
            "4\n" +
            "6\n" +
            "8\n"
        );
    }

    @Test
    public void testGenerateSeriesWithIntegerArguments() {
        assertExecute("generate_series(1::int, 3::int)", "1\n2\n3\n");
    }

    @Test
    public void testResultIsEmptyIfStartIsNull() {
        assertExecute("generate_series(null, 10)", "");
    }

    @Test
    public void testResultIsEmptyIfStopIsNull() {
        assertExecute("generate_series(1, null)", "");
    }

    @Test
    public void testResultIsEmptyIfStepIsNull() {
        assertExecute("generate_series(1, 2, null)", "");
    }

    @Test
    public void testStartIsReturnedIfStepIsBiggerThanStop() {
        assertExecute(
            "generate_series(1, 4, 8)",
            "1\n"
        );
    }

    @Test
    public void testEmptyOutputIfStartIsHigherThanStopWithDefaultStep() {
        assertExecute("generate_series(4294967294, 3)", "");
    }

    @Test
    public void testNegativeStepsWorkIfStartIsHigherThanStop() {
        assertExecute(
            "generate_series(4, 1, -1)",
            "4\n" +
            "3\n" +
            "2\n" +
            "1\n");
    }

    @Test
    public void testNegativeStartToPositiveEnd() {
        assertExecute(
            "generate_series(-2, 2, 1)",
            "-2\n" +
            "-1\n" +
            "0\n" +
            "1\n" +
            "2\n"
        );
    }

    @Test
    public void test_generate_series_with_more_than_long_max_rows() {
        Iterable<Row> rows = execute("generate_series(-9223372036854775807, 0)");
        assertThat(StreamSupport.stream(rows.spliterator(), false)
            .limit(3)
            .map(row -> row.get(0))
            .toList()
        ).containsExactly(-9223372036854775807L, -9223372036854775806L, -9223372036854775805L);
    }

    @Test
    public void test_generate_series_with_step_0() {
        assertThatThrownBy(() -> execute("generate_series(3, 5, 0)"))
            .hasMessage("step size cannot equal zero");
    }

    @Test
    public void test_generate_interval_with_step_0() {
        assertThatThrownBy(() -> execute("generate_series('2008-03-01 00:00'::timestamp, '2008-03-04 12:00'::timestamp, '0 hours'::interval)"))
            .hasMessage("step size cannot equal zero");
    }

    @Test
    public void test_generate_series_start_less_than_stop_step_negative() {
        assertExecute("generate_series(1, 4, -1)", "");
    }

    @Test
    public void test_generate_interval_start_less_than_stop_step_negative() {
        assertExecute("generate_series('2008-03-01 00:00'::timestamp, '2026-03-04 12:00'::timestamp, '-10 hours'::interval)", "");
    }

    @Test
    public void test_generate_interval_from_start_date_to_end_date_with_10_hours_interval() {
        assertExecute(
            "generate_series('2008-03-01 00:00'::timestamp, '2008-03-04 12:00'::timestamp, '10 hours'::interval)",
            "1204329600000\n" +
            "1204365600000\n" +
            "1204401600000\n" +
            "1204437600000\n" +
            "1204473600000\n" +
            "1204509600000\n" +
            "1204545600000\n" +
            "1204581600000\n" +
            "1204617600000\n"
        );
    }

    @Test
    public void test_generate_interval_from_start_date_to_end_date_with_minus_10_hours_interval() {
        assertExecute(
            "generate_series('2008-03-05 00:00'::timestamp, '2008-03-04 12:00'::timestamp, '-10 hours'::interval)",
            "1204675200000\n" +
            "1204639200000\n"
        );
    }

    @Test
    public void test_generate_series_with_interval_and_null_values_results_in_empty_result() {
        assertExecute(
            "generate_series(null::timestamp, '2019-03-04'::timestamp, '1 days'::interval)",
            ""
        );
    }

    @Test
    public void test_generate_series_with_start_after_end_and_positive_step_returns_empty_result() {
        assertExecute(
            "generate_series('2020-01-01'::timestamp, '2019-03-04'::timestamp, '1 days -4 hours'::interval)",
            ""
        );
    }

    @Test
    public void test_step_is_mandatory_for_timestamps() {
        Assertions.assertThatThrownBy(() -> assertExecute("generate_series(null::timestamptz, '2019-03-04'::timestamptz)", ""))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(
                    "generate_series(start, stop) has type `timestamp with time zone` for start, but requires long/int values for start and stop, or if used with timestamps, it requires a third argument for the step (interval)");
    }

    @Test
    public void test_generate_series_interval_keeps_milliseconds_of_start() {
        assertExecute(
            "generate_series('1970-01-01 00:00:00.500'::timestamp, '1970-01-01 00:00:02.500'::timestamp, '1 second'::interval)",
            """
                500
                1500
                2500
                """
        );
    }

    @Test
    public void test_generate_series_with_milliseconds_interval() {
        assertExecute(
            "generate_series('1970-01-01 00:00:00'::timestamp, '1970-01-01 00:00:00.300'::timestamp, '100 milliseconds'::interval)",
            """
                0
                100
                200
                300
                """
        );
    }

    @Test
    public void test_generate_series_stops_at_max_value() {
        Iterable<Row> rows = execute("generate_series(9223372036854775805, 9223372036854775807)");
        assertThat(StreamSupport.stream(rows.spliterator(), false)
            .limit(4)
            .map(row -> row.get(0))
            .toList()
        ).containsExactly(9223372036854775805L, 9223372036854775806L, 9223372036854775807L);

        rows = execute("generate_series(2147483645, 2147483647)");
        assertThat(StreamSupport.stream(rows.spliterator(), false)
            .limit(4)
            .map(row -> row.get(0))
            .toList()
        ).containsExactly(2147483645, 2147483646, 2147483647);
    }
}
