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

import static io.crate.testing.Asserts.assertThat;
import static org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

public class StorageOptionsITest extends IntegTestCase {

    @Test
    public void testInsertStringGreaterThanDocValuesLimit() throws Exception {
        execute("create table t1 (s string index off storage with (columnstore=false))");

        String bigString = RandomStrings.randomRealisticUnicodeOfLength(random(), MAX_TERM_LENGTH + 2);
        execute("insert into t1 (s) values (?)", new Object[]{bigString});
        execute("refresh table t1");

        execute("select s from t1 limit 1");
        assertThat(response.rows()[0][0]).isEqualTo(bigString);
    }

    @Test
    public void testAggregationWithDisabledDocValues() throws Exception {
        execute(
            """
                create table t1 (
                    i int storage with (columnstore=false),
                    s string storage with (columnstore=false)
                )""");

        execute("insert into t1 (i, s) values (?, ?), (?, ?)",
                new Object[][]{{1, "hello", 2, "foo"}});
        execute("refresh table t1");

        execute("select max(i), max(s) from t1");
        assertThat(response).hasRows("2| hello");
    }

    @Test
    public void testGroupByWithDisabledDocValues() throws Exception {
        execute(
            """
                create table t1 (
                    i int storage with (columnstore=false),
                    s string storage with (columnstore=false)
                )""");

        execute("insert into t1 (i, s) values (?, ?), (?, ?)",
                new Object[][]{{1, "hello", 2, "foo"}});
        execute("refresh table t1");

        execute("select count(s), count(i), i, s from t1 group by s, i order by i, s");
        assertThat(response).hasRows(
            "1| 1| 1| hello",
            "1| 1| 2| foo");
    }

    @Test
    public void testOrderByWithDisabledDocValues() throws Exception {
        execute(
            """
                create table t1 (
                    i int storage with (columnstore=false),
                    s string storage with (columnstore=false)
                )""");

        execute("insert into t1 (i, s) values (?, ?), (?, ?)",
                new Object[][]{{1, "foo", 2, "bar"}});
        execute("refresh table t1");

        execute("select s from t1 order by s");
        assertThat(response).hasRows(
            "bar",
            "foo");
        execute("select i from t1 order by i desc");
        assertThat(response).hasRows(
                "2",
                "1");
    }

    @Test
    public void test_timestamp_with_columnstore_false() throws Exception {
        execute("""
            create table tbl (
                ts timestamp without time zone storage with (columnstore=false),
                tsz timestamp with time zone storage with (columnstore=false)
            )""");
        execute("insert into tbl (ts, tsz) values (?, ?)", new Object[]{"2025-06-27 11:22:33.987", "2025-06-27 11:22:33.987+02:00"});
        execute("refresh table tbl");
        execute("select ts, tsz from tbl limit 1");
        assertThat((long) response.rows()[0][0]).isGreaterThan(0L); // Rough check due to randomized tz
        assertThat((long) response.rows()[0][1]).isEqualTo(1751016153987L);
    }

    @Test
    public void test_date_column_round_trip_with_and_without_columnstore() throws Exception {
        // End-to-end round-trip through the storage layer. Eq/range/IN/array query
        // generation is covered by DateEqQueryTest as unit tests; this IT pins the
        // pieces that need a real cluster: insert-then-select round-trip, ORDER BY
        // with NULL positioning (LuceneSort + NullSentinelValues), and the
        // columnstore=false read path (StorageSupport.decode(long)).
        execute("""
            create table tbl (
                id int primary key,
                d date,
                d_no_cs date storage with (columnstore=false)
            ) with (number_of_replicas = 0)
            """);
        execute("""
            insert into tbl (id, d, d_no_cs) values
                (1, '1815-12-10', '1815-12-10'),
                (2, '1970-01-01', '1970-01-01'),
                (3, '2026-06-08', '2026-06-08'),
                (4, NULL,         NULL)
            """);
        execute("refresh table tbl");

        // Equality round-trip through both default-storage and columnstore=false columns.
        execute("select id from tbl where d = '1970-01-01' and d_no_cs = '1970-01-01'");
        assertThat(response).hasRows("2");

        // ORDER BY with NULLS positioning - covers both branches of NullSentinelValues.
        execute("select id from tbl order by d asc nulls last");
        assertThat(response).hasRows("1", "2", "3", "4");

        execute("select id from tbl order by d desc nulls first");
        assertThat(response).hasRows("4", "3", "2", "1");

        // Same ORDER BY against the columnstore=false column — exercises the
        // stored-field sort path, which differs from the doc-values sort path used
        // for the default-storage column above.
        execute("select id from tbl order by d_no_cs asc nulls last");
        assertThat(response).hasRows("1", "2", "3", "4");

        // NULL is preserved through the storage layer.
        execute("select id from tbl where d is null");
        assertThat(response).hasRows("4");
    }
}
