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
import io.crate.sql.tree.ArraySliceExpression;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.TryCast;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.Optional;

import static io.crate.testing.TestingHelpers.isSQL;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class ArraySliceValidatorTest extends ESTestCase {

    private ArraySliceContext analyzeArraySlice(String expressionString) {
        ArraySliceContext context = new ArraySliceContext();
        Expression expression = SqlParser.createExpression(expressionString);
        ArraySliceValidator.validate((ArraySliceExpression) expression, context);
        return context;
    }

    @Test
    public void testVisitSubscriptExpression() {
        ArraySliceContext context = analyzeArraySlice("a.b[1:3]");
        assertThat("a", context.getQualifiedName().toString(), is("a.b"));
        assertThat(context.getFrom().get(), is(new IntegerLiteral(1)));
        assertThat(context.getTo().get(), is(new IntegerLiteral(3)));
    }

    @Test
    public void testSliceOnArrayLiteral() {
        ArraySliceContext context = analyzeArraySlice("[1,2,3][1:3]");
        assertThat(context.getBase(), is(notNullValue()));
        assertThat(context.getBase(), instanceOf(ArrayLiteral.class));
        assertThat(context.getFrom().get(), is(new IntegerLiteral(1)));
        assertThat(context.getTo().get(), is(new IntegerLiteral(3)));
    }

    @Test
    public void testSliceWithoutTo() {
        ArraySliceContext context = analyzeArraySlice("[1,2,3][1:]");
        assertThat(context.getBase(), is(notNullValue()));
        assertThat(context.getBase(), instanceOf(ArrayLiteral.class));
        assertThat(context.getFrom().get(), is(new IntegerLiteral(1)));
        assertThat(context.getTo(), is(Optional.empty()));
    }

    @Test
    public void testSliceWithoutFrom() {
        ArraySliceContext context = analyzeArraySlice("[1,2,3][:3]");
        assertThat(context.getBase(), is(notNullValue()));
        assertThat(context.getBase(), instanceOf(ArrayLiteral.class));
        assertThat(context.getFrom(), is(Optional.empty()));
        assertThat(context.getTo().get(), is(new IntegerLiteral(3)));
    }

    @Test
    public void testSliceWithoutFromAndTo() {
        ArraySliceContext context = analyzeArraySlice("[1,2,3][:]");
        assertThat(context.getBase(), is(notNullValue()));
        assertThat(context.getBase(), instanceOf(ArrayLiteral.class));
        assertThat(context.getFrom(), is(Optional.empty()));
        assertThat(context.getTo(), is(Optional.empty()));
    }

    @Test
    public void testSliceOnCast() {
        ArraySliceContext context = analyzeArraySlice("cast([1.1,2.1] as array(integer))[1:3]");
        assertThat(context.getBase(), is(notNullValue()));
        assertThat(context.getBase(), instanceOf(Cast.class));
        assertThat(context.getFrom().get(), is(new IntegerLiteral(1)));
        assertThat(context.getTo().get(), is(new IntegerLiteral(3)));
    }

    @Test
    public void testSliceOnTryCast() {
        ArraySliceContext context = analyzeArraySlice("try_cast([1, 2] as array(double))[1:3]");
        assertThat(context.getBase(), is(notNullValue()));
        assertThat(context.getBase(), instanceOf(TryCast.class));
        assertThat(context.getFrom().get(), is(new IntegerLiteral(1)));
        assertThat(context.getTo().get(), is(new IntegerLiteral(3)));
    }

    @Test
    public void testVisitArraySliceWithFromExpression() {
        ArraySliceContext context = analyzeArraySlice("a[1+2:]");
        assertThat(context.getQualifiedName().getSuffix(), is("a"));
        assertThat(context.getFrom().get(), isSQL("1 + 2"));
    }

    @Test
    public void testVisitArraySliceWithToExpression() {
        ArraySliceContext context = analyzeArraySlice("a[:1+2]");
        assertThat(context.getQualifiedName().getSuffix(), is("a"));
        assertThat(context.getTo().get(), isSQL("1 + 2"));
    }

    @Test
    public void testVisitArraySliceWithNestedFromExpression() throws Exception {
        ArraySliceContext context = analyzeArraySlice("a[b[1]:]");
        assertThat(context.getQualifiedName().getSuffix(), is("a"));
        assertThat(context.getFrom().get(), isSQL("\"b\"[1]"));
    }

    @Test
    public void testVisitArraySliceWithNestedToExpression() throws Exception {
        ArraySliceContext context = analyzeArraySlice("a[:b[1]]");
        assertThat(context.getQualifiedName().getSuffix(), is("a"));
        assertThat(context.getTo().get(), isSQL("\"b\"[1]"));
    }

    @Test
    public void testNestedArraySlice() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Nested array slice is not supported");
        analyzeArraySlice("a[1:2][3:4]");
    }

    @Test
    public void testInvalidArraySliceExpressionName() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("An expression of type StringLiteral cannot have an array slice ([<from>:<to>])");
        analyzeArraySlice("'a'[1:3]");
    }

    @Test
    public void testArraySliceBigTo() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        analyzeArraySlice("a[1:214748364700]");
    }

    @Test
    public void testArraySliceBigFrom() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        analyzeArraySlice("a[214748364700:5]");
    }

    @Test
    public void testArraySliceNegativeFrom() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        analyzeArraySlice("a[-1:5]");
    }

    @Test
    public void testArraySliceNegativeTo() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Array index must be in range 1 to 2147483647");
        analyzeArraySlice("a[1:-5]");
    }

    @Test
    public void testArraySliceFromParameter() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Parameter substitution is not supported in an array slice");
        analyzeArraySlice("a[?:5]");
    }

    @Test
    public void testArraySliceToParameter() {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Parameter substitution is not supported in an array slice");
        analyzeArraySlice("a[1:?]");
    }

}
