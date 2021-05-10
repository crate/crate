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

package io.crate.analyze;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.TryCast;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;


public class SubscriptValidatorTest extends ESTestCase {

    private SubscriptContext analyzeSubscript(String expressionString) {
        SubscriptContext context = new SubscriptContext();
        Expression expression = SqlParser.createExpression(expressionString);
        SubscriptValidator.validate((SubscriptExpression) expression, context);
        return context;
    }

    @Test
    public void testVisitSubscriptExpression() throws Exception {
        SubscriptContext context = analyzeSubscript("a['x']['y']");
        assertThat("a", context.qualifiedName().getSuffix(), is("a"));
        assertThat("x", context.parts().get(0), is("x"));
        assertThat("y", context.parts().get(1), is("y"));
    }

    @Test
    public void testInvalidSubscriptExpressionName() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("An expression of type StringLiteral cannot have an index accessor ([])");
        analyzeSubscript("'a'['x']['y']");
    }

    @Test
    public void testNestedSubscriptParameter() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Parameter substitution is not supported in subscript");
        analyzeSubscript("a['x'][?]");
    }

    @Test
    public void testArraySubscriptParameter() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Parameter substitution is not supported in subscript");
        analyzeSubscript("a[?]");
    }

    @Test
    public void testSubscriptOnArrayLiteral() throws Exception {
        SubscriptContext context = analyzeSubscript("[1,2,3][1]");
        assertThat(context.expression(), is(notNullValue()));
        assertThat(context.expression(), instanceOf(ArrayLiteral.class));
        assertThat(context.index(), isSQL("CAST(1 AS integer)"));
    }

    @Test
    public void testSubscriptOnCast() throws Exception {
        SubscriptContext context = analyzeSubscript("cast([1.1,2.1] as array(integer))[2]");
        assertThat(context.expression(), instanceOf(Cast.class));
        assertThat(context.index(), isSQL("CAST(2 AS integer)"));
    }

    @Test
    public void testSubscriptOnTryCast() throws Exception {
        SubscriptContext context = analyzeSubscript("try_cast([1] as array(double))[1]");
        assertThat(context.expression(), instanceOf(TryCast.class));
        assertThat(context.index(), isSQL("CAST(1 AS integer)"));
    }

    @Test
    public void testVisitSubscriptWithIndexExpression() throws Exception {
        SubscriptContext context = analyzeSubscript("a[1+2]");
        assertThat(context.qualifiedName().getSuffix(), is("a"));
        assertThat(context.index(), isSQL("1 + 2"));
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testVisitSubscriptWithNestedIndexExpression() throws Exception {
        SubscriptContext context = analyzeSubscript("a[a[abs(2-3)]]");
        assertThat(context.qualifiedName().getSuffix(), is("a"));
        context = analyzeSubscript(context.index().toString());
        assertThat(context.qualifiedName().getSuffix(), is("a"));
        assertThat(context.index(), isSQL("abs((2 - 3))"));
    }

    @Test
    public void testNestedArrayAccess() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Nested array access is not supported");
        analyzeSubscript("a[1][2]");
    }

    @Test
    public void testNegativeArrayAccess() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483648");
        analyzeSubscript("ref[-1]");
    }
}
