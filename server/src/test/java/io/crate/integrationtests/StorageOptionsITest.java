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
}
