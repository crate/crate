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
import static org.junit.Assert.assertThat;

import org.elasticsearch.test.IntegTestCase;
import org.hamcrest.core.Is;
import org.junit.Test;

import io.crate.testing.TestingHelpers;

public class OrderByITest extends IntegTestCase {

    @Test
    public void testOrderByIpType() throws Exception {
        execute("create table t1 (" +
                "  ipp ip" +
                ")");
        execute("insert into t1 (ipp) values (?)", new Object[][]{
            {"127.0.0.1"},
            {null},
            {"10.0.0.1"},
            {"10.200.1.100"},
            {"10.220.1.120"},
            {"10.220.1.20"}
        });
        execute("refresh table t1");
        execute("select ipp from t1 order by ipp");
        assertThat(TestingHelpers.printedTable(response.rows()), Is.is(
            "10.0.0.1\n" +
            "10.200.1.100\n" +
            "10.220.1.120\n" +
            "10.220.1.20\n" +
            "127.0.0.1\n" +
            "NULL\n"));

        execute("select ipp from t1 order by ipp desc nulls first");
        assertThat(TestingHelpers.printedTable(response.rows()), Is.is(
            "NULL\n" +
            "127.0.0.1\n" +
            "10.220.1.20\n" +
            "10.220.1.120\n" +
            "10.200.1.100\n" +
            "10.0.0.1\n"));
    }

    @Test
    public void testOrderByWithIndexOff() throws Exception {
        execute("create table t1 (s string index off)");
        execute("insert into t1 (s) values (?), (?)", new Object[]{"hello", "foo"});
        refresh();

        execute("select s from t1 order by s");
        assertThat(TestingHelpers.printedTable(response.rows())).isEqualTo("foo\n" +
                                                                    "hello\n");
    }
}
