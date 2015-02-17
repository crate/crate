/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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
import io.crate.sql.tree.Expression;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;


public class SubscriptVisitorTest {

    public SubscriptVisitor visitor = new SubscriptVisitor();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private SubscriptContext analyzeSubscript(String expressionString) {
        ParameterContext parameterContext = mock(ParameterContext.class);
        SubscriptContext context = new SubscriptContext(parameterContext);
        Expression expression = SqlParser.createExpression(expressionString);
        expression.accept(visitor, context);
        return context;
    }

    @Test
    public void testVisitSubscriptExpression() throws Exception {
        SubscriptContext context = analyzeSubscript("a['x']['y']");

        assertEquals("a", context.qName().getSuffix());
        assertEquals("x", context.parts().get(0));
        assertEquals("y", context.parts().get(1));
    }

    @Test
    public void testInvalidSubscriptExpressionName() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("An expression of type StringLiteral cannot have an index accessor ([])");

        analyzeSubscript("'a'['x']['y']");
    }

    @Test
    public void testInvalidSubscriptExpressionIndex() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("index of subscript has to be a string or long literal or parameter. Any other index expression is not supported");

        analyzeSubscript("a[x]['y']");
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
        assertThat(context.index(), is(1));
    }

    @Test
    public void testStringSubscriptOnArrayLiteral() throws Exception {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Array literals can only be accessed via numeric index.");

        analyzeSubscript("[1,2,3]['abc']");
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
