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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import io.crate.testing.TestingHelpers;
import org.junit.Test;

import static org.apache.lucene.index.IndexWriter.MAX_TERM_LENGTH;
import static org.hamcrest.Matchers.is;

public class StorageOptionsITest extends SQLIntegrationTestCase {

    @Test
    public void testInsertStringGreaterThanDocValuesLimit() throws Exception {
        execute("create table t1 (s string index off storage with (columnstore=false))");

        String bigString = RandomStrings.randomRealisticUnicodeOfLength(random(), MAX_TERM_LENGTH + 2);
        execute("insert into t1 (s) values (?)", new Object[]{bigString});
        refresh();

        execute("select s from t1 limit 1");
        assertThat(response.rows()[0][0], is(bigString));
    }

    @Test
    public void testAggregationWithDisabledDocValues() throws Exception {
        execute("create table t1 (s string storage with (columnstore=false))");

        execute("insert into t1 (s) values (?), (?)", new Object[]{"hello", "foo"});
        refresh();

        execute("select max(s) from t1");
        assertThat(response.rows()[0][0], is("hello"));
    }

    @Test
    public void testGroupByWithDisabledDocValues() throws Exception {
        execute("create table t1 (s string index off storage with (columnstore=false))");

        execute("insert into t1 (s) values (?), (?)", new Object[]{"hello", "foo"});
        refresh();

        execute("select count(s), s from t1 group by s order by s");
        assertThat(TestingHelpers.printedTable(response.rows()), is("1| foo\n" +
                                                                    "1| hello\n"));
    }

    @Test
    public void testOrderByWithDisabledDocValues() throws Exception {
        execute("create table t1 (s string storage with (columnstore=false))");
        execute("insert into t1 (s) values (?), (?)", new Object[]{"foo", "bar"});
        refresh();

        execute("select s from t1 order by s");
        assertThat(TestingHelpers.printedTable(response.rows()), is("bar\n" +
                                                                    "foo\n"));
    }
}
