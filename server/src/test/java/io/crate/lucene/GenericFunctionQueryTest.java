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

package io.crate.lucene;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.test.integration.CrateDummyClusterServiceUnitTest;
import io.crate.testing.QueryTester;


public class GenericFunctionQueryTest extends CrateDummyClusterServiceUnitTest {

    @Test
    public void test_generic_function_query_cannot_be_cached_with_un_deterministic_functions_present() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (x int)"
        );
        builder.indexValues("x", 1, 2, 3);
        try (QueryTester tester = builder.build()) {
            var query = tester.toQuery("x = random()");
            var searcher = tester.searcher();
            var weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            assertThat(weight.isCacheable(searcher.getTopReaderContext().leaves().get(0))).isFalse();
        }
    }

    @Test
    public void test_generic_function_query_can_be_cached_if_deterministic() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table t (x int)"
        );
        builder.indexValues("x", 1, 2, 3);
        try (QueryTester tester = builder.build()) {
            var query = tester.toQuery("abs(x) = 1");
            var searcher = tester.searcher();
            var weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1.0f);
            assertThat(weight.isCacheable(searcher.getTopReaderContext().leaves().get(0))).isTrue();
            assertThat(tester.runQuery("x", "abs(x) = 1")).containsExactly(1);
        }
    }
}
