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

package io.crate.expression.operator;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.lucene.GenericFunctionQuery;
import io.crate.metadata.IndexType;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.testing.Asserts;
import io.crate.testing.DataTypeTesting;
import io.crate.testing.QueryTester;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;

public class EqOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeSymbol() {
        assertNormalize("2 = 2", isLiteral(true));
    }

    @Test
    public void testEqArrayLeftSideIsNull_RightSideNull() throws Exception {
        assertEvaluateNull("[1, 10] = null");
        assertEvaluateNull("null = [1, 10]");
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsTrueIfEquals() throws Exception {
        assertNormalize("[ [1, 1], [10] ] = [ [1, 1], [10] ]", isLiteral(true));
    }

    @Test
    public void testNormalizeEvalNestedIntArrayIsFalseIfNotEquals() throws Exception {
        assertNormalize("[ [1, 1], [10] ] = [ [1], [10] ]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoEqualArraysShouldReturnTrueLiteral() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 1, 10]", isLiteral(true));
    }

    @Test
    public void testNormalizeAndEvalTwoNotEqualArraysShouldReturnFalse() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeAndEvalTwoArraysWithSameLengthButDifferentValuesShouldReturnFalse() throws Exception {
        assertNormalize("[1, 1, 10] = [1, 2, 10]", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolWithNullLiteral() {
        assertNormalize("null = null", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbolWithOneNullLiteral() {
        assertNormalize("2 = null", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbolNeq() {
        assertNormalize("2 = 4", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolNonLiteral() {
        assertNormalize("name = 'Arthur'", isFunction(EqOperator.NAME));
    }

    @Test
    public void testEvaluateEqOperator() {
        assertNormalize("{l=1, b=true} = {l=1, b=true}", isLiteral(true));
        assertNormalize("{l=2, b=true} = {l=1, b=true}", isLiteral(false));
        assertNormalize("{l=2, b=true} = {}", isLiteral(false));

        assertNormalize("1.2 = null", isLiteral(null));
        assertNormalize("'foo' = null", isLiteral(null));
    }

    @Test
    public void test_array_equals_empty_array_on_all_types() throws Exception {
        for (DataType<?> type : DataTypeTesting.ALL_STORED_TYPES_EXCEPT_ARRAYS) {
            if (type instanceof FloatVectorType) {
                continue;
            }
            // Universal values for all types, '=[]' should match 1 row for all types.
            // Also covers cases when we need to add extra generic filter to differentiate between null and empty array.
            Object[] values = new Object[] {List.of(), null};

            // ensure the test is operating on a fresh, empty cluster state (no tables)
            resetClusterService();

            try (QueryTester tester = new QueryTester.Builder(
                createTempDir(),
                THREAD_POOL,
                clusterService,
                Version.CURRENT,
                "create table \"t_" + type.getName() + "\" (xs array(\"" + type.getName() + "\"))"
            ).indexValues("xs", values).build()) {
                List<Object> result = tester.runQuery("xs", "xs = []");
                Asserts.assertThat(result)
                    .as("xs = [] should match 1 row for type " + type)
                    .hasSize(1);
                Asserts.assertThat(result.get(0)).asList().isEmpty();
            }
        }
    }

    @Test
    public void test_terms_query_on__id_encodes_ids() throws Exception {
        Query query = EqOperator.termsQuery(DocSysColumns.ID.name(), DataTypes.STRING, List.of("foo", "bar"), true, IndexType.PLAIN);
        assertThat(query).hasToString("_id:([7e 8a] [ff 62 61 72])");
        query = EqOperator.termsQuery(DocSysColumns.ID.name(), DataTypes.STRING, List.of("foo", "bar"), false, IndexType.PLAIN);
        assertThat(query).hasToString("_id:([7e 8a] [ff 62 61 72])");
        query = EqOperator.termsQuery(DocSysColumns.ID.name(), DataTypes.STRING, List.of("foo", "bar"), true, IndexType.NONE);
        assertThat(query).hasToString("_id:([7e 8a] [ff 62 61 72])");
        query = EqOperator.termsQuery(DocSysColumns.ID.name(), DataTypes.STRING, List.of("foo", "bar"), false, IndexType.NONE);
        assertThat(query).hasToString("_id:([7e 8a] [ff 62 61 72])");
    }

    @Test
    public void test_terms_query_on_empty_object() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            createTempDir(),
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (obj object)");
        builder.indexValue("obj", Map.of());
        builder.indexValue("obj", null);
        try (QueryTester tester = builder.build()) {
            Query query = tester.toQuery("obj = {}");
            assertThat(query)
                .isExactlyInstanceOf(GenericFunctionQuery.class)
                .hasToString("(obj = {})");

            assertThat(tester.runQuery("obj", "obj = {}")).containsExactly(
                Map.of()
            );
        }
    }
}
