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

package io.crate.expression.operator.any;

import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.testing.QueryTester;

public class AnyEqOperatorTest extends ScalarTestCase {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("1 = ANY([1])", true);
        assertEvaluate("1 = ANY([2])", false);

        assertEvaluate("{i=1, b=true} = ANY([{i=1, b=true}])", true);
        assertEvaluate("{i=1, b=true} = ANY([{i=2, b=true}])", false);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluateNull("null = ANY(null)");
        assertEvaluateNull("42 = ANY(null)");
        assertEvaluateNull("null = ANY([1])");
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null = ANY([null])", isLiteral(null));
        assertNormalize("42 = ANY([null])", isLiteral(null));
        assertNormalize("null = ANY([1])", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("42 = ANY([42])", isLiteral(true));
        assertNormalize("42 = ANY([1, 42, 2])", isLiteral(true));
        assertNormalize("42 = ANY([42])", isLiteral(true));
        assertNormalize("42 = ANY([41, 43, -42])", isLiteral(false));
    }

    @Test
    public void testArrayEqAnyNestedArrayMatches() {
        assertNormalize("['foo', 'bar'] = ANY([ ['foobar'], ['foo', 'bar'], [] ])", isLiteral(true));
    }

    @Test
    public void testArrayEqAnyNestedArrayDoesNotMatch() {
        assertNormalize("['foo', 'bar'] = ANY([ ['foobar'], ['foo', 'ar'], [] ])", isLiteral(false));
    }

    @Test
    public void test_uses_terms_query_for_unnested_array_refs() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (obj_arr array(object as (xs integer[])))"
        );
        List<Map<String, List<Integer>>> value1 = List.of(
            Map.of("xs", List.of(1, 2)),
            Map.of("xs", List.of(3, 4))
        );
        List<Map<String, List<Integer>>> value2 = List.of(
            Map.of("xs", List.of(5, 6))
        );
        builder.indexValues("obj_arr", value1, value2);
        try (var queryTester = builder.build()) {
            assertThat(queryTester.runQuery("obj_arr", "1 = ANY(obj_arr['xs'])"))
                .containsExactly(value1);

            assertThat(queryTester.runQuery("obj_arr", "[1, 2] = ANY(obj_arr['xs'])"))
                .containsExactly(value1);

            assertThat(queryTester.runQuery("obj_arr", "6 = ANY(obj_arr['xs'])"))
                .containsExactly(value2);
            assertThat(queryTester.runQuery("obj_arr", "9 = ANY(obj_arr['xs'])"))
                .hasSize(0);
            Query query = queryTester.toQuery("1 = ANY(obj_arr['xs'])");
            assertThat(query.toString()).isEqualTo("obj_arr.xs:[1 TO 1]");
        }
    }
}
