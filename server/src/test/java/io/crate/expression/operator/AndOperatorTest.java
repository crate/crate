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

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Symbol;
import org.junit.Test;

import java.util.List;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static io.crate.testing.SymbolMatchers.isReference;
import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.contains;

public class AndOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalizeBooleanTrueAndNonLiteral() throws Exception {
        assertNormalize("is_awesome and true", isReference("is_awesome"));
    }

    @Test
    public void testNormalizeBooleanFalseAndNonLiteral() throws Exception {
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
        assertEvaluate("true and null", null);
        assertEvaluate("null and true", null);
        assertEvaluate("false and null", false);
        assertEvaluate("null and false", false);
        assertEvaluate("null and null", null);
    }

    @Test
    public void test_get_conjunctions_of_predicate_with_2_ands() {
        Symbol query = sqlExpressions.asSymbol("(a = 1::int or a = 2::int) AND x = 2::int AND name = 'foo'");
        List<Symbol> split = AndOperator.split(query);
        assertThat(split, contains(
            isSQL("((doc.users.a = 1) OR (doc.users.a = 2))"),
            isSQL("(doc.users.x = 2::bigint)"),
            isSQL("(doc.users.name = 'foo')")
        ));
    }

    @Test
    public void test_get_conjunctions_of_predicate_without_any_ands() {
        Symbol query = sqlExpressions.asSymbol("a = 1::int");
        List<Symbol> split = AndOperator.split(query);
        assertThat(split, contains(
            isSQL("(doc.users.a = 1)")
        ));
    }
}
