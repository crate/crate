/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import static io.crate.testing.TestingHelpers.printedTable;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class ColumnPositionIntegrationTest extends SQLIntegrationTestCase {

    @Test
    public void test_column_positions_from_create_table() {
        execute(
            """
                create table t (
                    ta boolean,
                    tb text,
                    tc object(dynamic) as (
                        td text,
                        te timestamp with time zone,
                        tf object(dynamic) as (
                            tg integer
                        )
                    ),
                    INDEX th using fulltext(tb, tc['td']),
                    ti ARRAY(OBJECT AS (tj INTEGER, tk TEXT)),
                    tl ARRAY(GEO_POINT)
                )
                """
        );
        execute("""
                    select column_name, ordinal_position
                    from information_schema.columns
                    where column_name like 't_' or column_name like 't_[%'
                    order by 2""");
        assertThat(printedTable(response.rows()), is("""
                                                         ta| 1
                                                         tb| 2
                                                         tc| 3
                                                         tc['td']| 4
                                                         tc['te']| 5
                                                         tc['tf']| 6
                                                         tc['tf']['tg']| 7
                                                         ti| 8
                                                         ti['tj']| 9
                                                         ti['tk']| 10
                                                         tl| 11
                                                         """));
    }

    @Test
    public void test_column_positions_after_multiple_dynamic_inserts() {
        // series of dynamic inserts can help make sure that already assigned column positions are not modified.

        execute(
            """
                create table t (
                    tb array(object(dynamic)),
                    ta object(dynamic)
                ) with (column_policy = 'dynamic');
                """
        );
        execute("""
                    select column_name, ordinal_position
                    from information_schema.columns
                    where column_name like 't_' or column_name like 't_[%'
                    order by 2""");
        assertThat(printedTable(response.rows()), is("""
                                                         tb| 1
                                                         ta| 2
                                                         """));

        // dynamic insert 1
        execute("insert into t (tc, ta) values ([1,2,3], {td = 1, te = {tf = false}})");
        execute("""
                    select column_name, ordinal_position
                    from information_schema.columns
                    where column_name like 't_' or column_name like 't_[%'
                    order by 2""");
        assertThat(printedTable(response.rows()), is("""
                                                         tb| 1
                                                         ta| 2
                                                         ta['td']| 3
                                                         ta['te']| 4
                                                         ta['te']['tf']| 5
                                                         tc| 6
                                                         """));

        // dynamic insert 2
        execute("insert into t (td, tb, ta, tz) values (2, [{t1 = 1, t2 = 2}, {t3 = 3}], {te = {ti = 5}}, 'z')");
        execute("""
                    select column_name, ordinal_position
                    from information_schema.columns
                    where column_name like 't_' or column_name like 't_[%'
                    order by 2""");
        assertThat(printedTable(response.rows()), is("""
                                                         tb| 1
                                                         ta| 2
                                                         ta['td']| 3
                                                         ta['te']| 4
                                                         ta['te']['tf']| 5
                                                         tc| 6
                                                         td| 7
                                                         tz| 8
                                                         ta['te']['ti']| 9
                                                         tb['t1']| 10
                                                         tb['t2']| 11
                                                         tb['t3']| 12
                                                         """));
    }
}
