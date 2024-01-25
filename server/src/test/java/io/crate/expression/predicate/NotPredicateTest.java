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

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.elasticsearch.Version;
import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.testing.QueryTester;
import io.crate.types.DataTypes;

public class NotPredicateTest extends ScalarTestCase {

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("not null", isLiteral(null, DataTypes.BOOLEAN));
    }

    @Test
    public void testNormalizeSymbolBoolean() throws Exception {
        assertNormalize("not true", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("not name = 'foo'", isFunction(NotPredicate.NAME));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("not name = 'foo'", false, Literal.of("foo"));
    }

    @Test
    public void test_not_on_case_uses_strict_3vl() throws Exception {
        QueryTester.Builder builder = new QueryTester.Builder(
            THREAD_POOL,
            clusterService,
            Version.CURRENT,
            "create table tbl (x int)"
        );
        builder.indexValue("x", null);
        builder.indexValue("x", 2);

        try (var tester = builder.build()) {
            List<Object> result = tester.runQuery("x", "(case when true then 2 else x end) != 1");
            assertThat(result).containsExactly(null, 2);

            result = tester.runQuery("x", "(case when true then 2 else x end) != 2");
            assertThat(result).isEmpty();
        }
    }

    @Test
    public void test_normalize_not_null_ref_to_true() {
        assertNormalize("b is not null", isLiteral(true));
    }
}
