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

import static io.crate.testing.Asserts.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.TryCast;


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
        assertThat(context.qualifiedName().getSuffix()).as("a").isEqualTo("a");
        assertThat(context.parts().get(0)).as("x").isEqualTo("x");
        assertThat(context.parts().get(1)).as("y").isEqualTo("y");
    }

    @Test
    public void testInvalidSubscriptExpressionName() throws Exception {
        assertThatThrownBy(() -> analyzeSubscript("'a'['x']['y']"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("An expression of type StringLiteral cannot have an index accessor ([])");
    }

    @Test
    public void testNestedSubscriptParameter() throws Exception {
        assertThatThrownBy(() -> analyzeSubscript("a['x'][?]"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Parameter substitution is not supported in subscript index");
    }

    @Test
    public void testArraySubscriptParameter() throws Exception {
        assertThatThrownBy(() -> analyzeSubscript("a[?]"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Parameter substitution is not supported in subscript index");
    }

    @Test
    public void testSubscriptOnArrayLiteral() throws Exception {
        SubscriptContext context = analyzeSubscript("[1,2,3][1]");
        assertThat(context.expression()).isNotNull();
        assertThat(context.expression()).isExactlyInstanceOf(ArrayLiteral.class);
        assertThat(context.index()).isSQL("1");
    }

    @Test
    public void testSubscriptOnCast() throws Exception {
        SubscriptContext context = analyzeSubscript("cast([1.1,2.1] as array(integer))[2]");
        assertThat(context.expression()).isExactlyInstanceOf(Cast.class);
        assertThat(context.index()).isSQL("2");
    }

    @Test
    public void testSubscriptOnTryCast() throws Exception {
        SubscriptContext context = analyzeSubscript("try_cast([1] as array(double))[1]");
        assertThat(context.expression()).isExactlyInstanceOf(TryCast.class);
        assertThat(context.index()).isSQL("1");
    }

    @Test
    public void testVisitSubscriptWithIndexExpression() throws Exception {
        SubscriptContext context = analyzeSubscript("a[1+2]");
        assertThat(context.qualifiedName().getSuffix()).isEqualTo("a");
        assertThat(context.index()).isSQL("1 + 2");
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testVisitSubscriptWithNestedIndexExpression() throws Exception {
        SubscriptContext context = analyzeSubscript("a[a[abs(2-3)]]");
        assertThat(context.qualifiedName().getSuffix()).isEqualTo("a");
        context = analyzeSubscript(context.index().toString());
        assertThat(context.qualifiedName().getSuffix()).isEqualTo("a");
        assertThat(context.index()).isSQL("abs((2 - 3))");
    }

    @Test
    public void testNestedArrayAccess() throws Exception {
        assertThatThrownBy(() -> analyzeSubscript("a[1][2]"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Nested array access is not supported");
        assertThatThrownBy(() -> analyzeSubscript("a[2147483648][1]"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Nested array access is not supported");
    }
}
