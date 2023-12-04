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

package io.crate.expression.scalar;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.testing.QueryTester;

public class NullOrEmptyFunctionTest extends ScalarTestCase {

    @Test
    public void test_null_or_empty_on_object() throws Exception {
        assertEvaluate("null_or_empty(null::object)", true);
        assertEvaluate("null_or_empty({a = 10})", false);
        assertEvaluate("null_or_empty({})", true);

        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (obj object as (x int))"
        );
        builder.indexValues("obj", new Object[] { null, Map.of(), Map.of("x", 1) });
        try (var queryTester = builder.build()) {
            assertThat(queryTester.toQuery("null_or_empty(obj)").toString())
                .isEqualTo("+*:* -((FieldExistsQuery [field=obj.x])~1)");
            List<Object> results = queryTester.runQuery("obj", "null_or_empty(obj)");
            assertThat(results).containsExactlyInAnyOrder(null, Map.of());

            results = queryTester.runQuery("obj", "not null_or_empty(obj)");
            assertThat(results).containsExactlyInAnyOrder(Map.of("x", 1));
        }
    }

    @Test
    public void test_null_or_empty_on_array() throws Exception {
        assertEvaluate("null_or_empty(null::text[])", true);
        assertEvaluate("null_or_empty(['foo', 'bar'])", false);
        assertEvaluate("null_or_empty([])", true);

        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (arr integer[])"
        );
        builder.indexValues("arr", new Object[] { null, List.of(), List.of(1, 2, 3) });
        try (var queryTester = builder.build()) {
            assertThat(queryTester.toQuery("null_or_empty(arr)").toString())
                .isEqualTo("+*:* -FieldExistsQuery [field=arr]");
            List<Object> results = queryTester.runQuery("arr", "null_or_empty(arr)");
            assertThat(results).containsExactlyInAnyOrder(null, List.of());

            results = queryTester.runQuery("arr", "not null_or_empty(arr)");
            assertThat(results).containsExactlyInAnyOrder(List.of(1, 2, 3));
        }
    }
}
