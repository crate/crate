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

package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;

import java.util.List;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.types.DataTypes;

public class AnalyzeITest extends IntegTestCase {

    @Test
    @SuppressWarnings("unchecked")
    public void test_analyze_statement_refreshes_table_stats_and_stats_are_visible_in_pg_class_and_pg_stats() {
        execute("create table doc.tbl (x int)");
        execute("insert into doc.tbl (x) values (1), (2), (3), (null), (3), (3)");
        execute("refresh table doc.tbl");
        execute("analyze");
        execute("select reltuples from pg_class where relname = 'tbl'");
        assertThat(response.rows()[0][0]).isEqualTo(6.0f);

        execute(
            "select " +
            "   null_frac," +
            "   avg_width," +
            "   n_distinct," +
            "   most_common_vals," +
            "   most_common_freqs," +
            "   histogram_bounds " +
            "from pg_stats where tablename = 'tbl'");
        Object[] row = response.rows()[0];
        assertThat(((Float) row[0]).doubleValue()).isCloseTo(0.166, withPercentage(1));
        assertThat(row[1]).isEqualTo(DataTypes.INTEGER.fixedSize());
        assertThat(row[2]).isEqualTo(3.0f);
        assertThat(((List<String>) row[3])).containsExactly("3", "2", "1");
        assertThat(((List<Float>) row[4])).hasSize(3);
        assertThat(((List<Float>) row[4]).get(0)).isEqualTo(0.5f);
        assertThat(((List<String>) row[5])).isEmpty();
    }

    @Test
    public void test_analyze_statement_works_on_tables_with_object_arrays() throws Exception {
        execute("create table doc.tbl (objs array(object as (y int)))");
        execute("insert into doc.tbl (objs) values ([{y=1}, {y=2}])");
        execute("refresh table doc.tbl");
        execute("analyze");
        execute("select reltuples from pg_class where relname = 'tbl'");
        assertThat(response.rows()[0][0]).isEqualTo(1.0f);
    }

    @Test
    public void test_select_from_pg_stats_when_most_common_vals_is_array_type_value() {
        execute("CREATE TABLE doc.tbl (val array(object as (id int)))");
        execute("INSERT INTO doc.tbl (val) VALUES ([{id=1}]), ([{id=1}]), ([{id=2}])");
        execute("REFRESH TABLE doc.tbl");
        execute("ANALYZE");
        execute("SELECT attname, most_common_vals, histogram_bounds FROM pg_stats");
        Object[] row = response.rows()[0];
        assertThat(row[0]).isEqualTo("val['id']");
        assertThat(row[1]).isEqualTo(List.of("[1]", "[2]"));
        assertThat(row[2]).isEqualTo(List.of());
    }
}
