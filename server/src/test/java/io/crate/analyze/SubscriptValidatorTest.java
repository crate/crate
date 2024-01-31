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
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.IntegerLiteral;


public class SubscriptValidatorTest extends ESTestCase {

    private SubscriptContext analyzeSubscript(String expressionString) {
        Expression expression = SqlParser.createExpression(expressionString);
        return SubscriptVisitor.visit(expression);
    }

    @Test
    public void testVisitSubscriptExpression() {
        SubscriptContext context = analyzeSubscript("a['x']['y']");
        assertThat(context.qualifiedName().getSuffix()).as("a").isEqualTo("a");
        assertThat(context.parts().get(0)).as("x").isEqualTo("x");
        assertThat(context.parts().get(1)).as("y").isEqualTo("y");
    }

    @Test
    public void testInvalidSubscriptExpressionName() {
        assertThatThrownBy(() -> analyzeSubscript("'a'['x']['y']"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("An expression of type StringLiteral cannot have an index accessor ([])");
    }

    @Test
    public void testNestedSubscriptParameter() {
        assertThatThrownBy(() -> analyzeSubscript("a['x'][?]"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Parameter substitution is not supported in subscript index");
    }

    @Test
    public void testArraySubscriptParameter() {
        assertThatThrownBy(() -> analyzeSubscript("a[?]"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage("Parameter substitution is not supported in subscript index");
    }

    @Test
    public void testSubscriptOnArrayLiteral() {
        SubscriptContext context = analyzeSubscript("[1,2,3][1]");
        assertThat(context.hasExpression()).isTrue();
        assertThat(context.index().get(0)).isSQL("1");
    }

    @Test
    public void testSubscriptOnCast() {
        SubscriptContext context = analyzeSubscript("cast([1.1,2.1] as array(integer))[2]");
        assertThat(context.hasExpression()).isTrue();
        assertThat(context.index().get(0)).isSQL("2");
    }

    @Test
    public void testSubscriptOnTryCast() {
        SubscriptContext context = analyzeSubscript("try_cast([1] as array(double))[1]");
        assertThat(context.hasExpression()).isTrue();
        assertThat(context.index().get(0)).isSQL("1");
    }

    @Test
    public void testVisitSubscriptWithIndexExpression() {
        SubscriptContext context = analyzeSubscript("a[1+2]");
        assertThat(context.qualifiedName().getSuffix()).isEqualTo("a");
        assertThat(context.index().get(0)).isSQL("1 + 2");
    }

    @SuppressWarnings("ConstantConditions")
    @Test
    public void testVisitSubscriptWithNestedIndexExpression() {
        SubscriptContext context = analyzeSubscript("a[a[abs(2-3)]]");
        assertThat(context.qualifiedName().getSuffix()).isEqualTo("a");
        context = analyzeSubscript(context.index().get(0).toString());
        assertThat(context.qualifiedName().getSuffix()).isEqualTo("a");
        assertThat(context.index().get(0)).isSQL("abs((2 - 3))");
    }

    @Test
    public void testNestedArrayAccess() {
        SubscriptContext context = analyzeSubscript("a[1][2][3]");
        assertThat(context.index()).hasSize(3);
        assertThat(context.index()).containsExactly(
            new IntegerLiteral(1),
            new IntegerLiteral(2),
            new IntegerLiteral(3)
        );
    }
}
