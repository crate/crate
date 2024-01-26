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

import static io.crate.testing.Asserts.assertList;
import static io.crate.testing.Asserts.isLiteral;
import static io.crate.testing.Asserts.isReference;

import java.util.List;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Symbol;

public class AndOperatorTest extends ScalarTestCase {

    @Test
    public void test_normalize_boolean_true_and_reference() throws Exception {
        assertNormalize("is_awesome and true", isReference("is_awesome"));
    }

    @Test
    public void test_normalize_boolean_false_and_reference() throws Exception {
        assertNormalize("is_awesome and false", isLiteral(false));
    }

    @Test
    public void testNormalizeLiteralAndLiteral() throws Exception {
        assertNormalize("true and true", isLiteral(true));
    }

    @Test
    public void testNormalizeLiteralAndLiteralFalse() throws Exception {
        assertNormalize("true and false", isLiteral(false));
    }

    @Test
    public void testEvaluateAndOperator() {
        assertEvaluate("true and true", true);
        assertEvaluate("false and false", false);
        assertEvaluate("true and false", false);
        assertEvaluate("false and true", false);
        assertEvaluateNull("true and null");
        assertEvaluateNull("null and true");
        assertEvaluate("false and null", false);
        assertEvaluate("null and false", false);
        assertEvaluateNull("null and null");
    }

    @Test
    public void test_get_conjunctions_of_predicate_with_2_ands() {
        Symbol query = sqlExpressions.asSymbol("(a = 1::int or a = 2::int) AND x = 2::int AND name = 'foo'");
        List<Symbol> split = AndOperator.split(query);
        assertList(split).isSQL("((doc.users.a = 1) OR (doc.users.a = 2)), " +
                                "(doc.users.x = 2::bigint), " +
                                "(doc.users.name = 'foo')");
    }

    @Test
    public void test_get_conjunctions_of_predicate_without_any_ands() {
        Symbol query = sqlExpressions.asSymbol("a = 1::int");
        List<Symbol> split = AndOperator.split(query);
        assertList(split).isSQL("(doc.users.a = 1)");
    }
}
