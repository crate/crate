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


package io.crate.expression.predicate;

import org.elasticsearch.Version;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;

public class IsNullQueryTest extends CrateDummyClusterServiceUnitTest {

    private void assertOnlyNullMatches(String createTable) throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            createTable
        );
        Object[] values = new Object[] {
            new Object[] { 1, 2, 3 },
            new Object[] {},
            null
        };
        builder.indexValues("xs", values);
        try (var queryTester = builder.build()) {
            var results = queryTester.runQuery("xs", "xs is null");
            assertThat(results.size(), Matchers.is(1));
            assertThat(results.get(0), Matchers.nullValue());
        }
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays() throws Exception {
        assertOnlyNullMatches("create table t (xs text[])");
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays_with_index_off() throws Exception {
        assertOnlyNullMatches("create table t (xs text[] index off)");
    }

    @Test
    public void test_is_null_does_not_match_empty_arrays_with_index_and_column_store_off() throws Exception {
        assertOnlyNullMatches("create table t (xs text[] index off storage with (columnstore = false))");
    }
}
