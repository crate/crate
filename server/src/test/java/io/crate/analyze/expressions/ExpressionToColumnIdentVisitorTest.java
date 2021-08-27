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

package io.crate.analyze.expressions;

import io.crate.exceptions.InvalidColumnNameException;
import io.crate.metadata.ColumnIdent;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class ExpressionToColumnIdentVisitorTest extends ESTestCase {

    @Test
    public void testConvertWithSimpleStringLiteral() throws Exception {
        assertIllegalArgumentExceptionOnConvert("'a'", "String literal");
    }

    @Test
    public void testConvertWithArithmeticStringLiteral() throws Exception {
        assertIllegalArgumentExceptionOnConvert("'1+1'", "String literal");
    }

    @Test
    public void testConvertWithDotStringLiteral() throws Exception {
        assertIllegalArgumentExceptionOnConvert("'a.b'", "String literal");
    }

    @Test
    public void testConvertWithSubscriptStringLiteral() throws Exception {
        assertIllegalArgumentExceptionOnConvert("'a[b]'", "String literal");
    }

    @Test
    public void testConvertWithQualifiedNameReferenceContainingDot() throws Exception {
        assertIllegalArgumentExceptionOnConvert(
            "a.b",
            "Qualified name references are not allowed to contain paths"
        );
    }

    @Test
    public void testConvertWithQualifiedNameReferenceContainingDots() throws Exception {
        assertIllegalArgumentExceptionOnConvert(
            "a.b.c",
            "Qualified name references are not allowed to contain paths"
        );
    }

    @Test
    public void testConvertWithQualifiedNameReference() throws Exception {
        assertColumnIdent("a", new ColumnIdent("a"));
        assertColumnIdent("ab", new ColumnIdent("ab"));
        assertColumnIdent("a_b", new ColumnIdent("a_b"));
    }

    @Test
    public void testConvertWithSubscripts() throws Exception {
        assertColumnIdent("a['b']", new ColumnIdent("a", "b"));
        assertColumnIdent("a['b']['c']", ColumnIdent.fromPath("a.b.c"));

        assertIllegalArgumentExceptionOnConvert("a[b.c]", "Key of subscript must not be a reference");
    }

    @Test
    public void testConvertWithInvalidObjectColumnName() throws Exception {
        assertInvalidColumnNameExceptionOnConvert("a['b.c']", "contains a dot");
    }

    @Test
    public void testConvertWithInvalidSubscriptObjectColumnName() throws Exception {
        assertInvalidColumnNameExceptionOnConvert("a['b[0]']", "subscript pattern");
    }

    @Test
    public void testConvertWithUnsupportedSubscript() throws Exception {
        assertExceptionOnUnsupportedConvert("a[0]");
    }

    @Test
    public void testConvertWithUnsupportedArithmetic() throws Exception {
        assertExceptionOnUnsupportedConvert("1+1");
    }

    @Test
    public void testConvertWithUnsupportedLong() throws Exception {
        assertExceptionOnUnsupportedConvert("1");
    }

    private void assertColumnIdent(String stringValue, ColumnIdent expecedIdent) {
        assertEquals(createIdentFromString(stringValue), expecedIdent);
    }

    private ColumnIdent createIdentFromString(String stringValue) {
        Expression expression = SqlParser.createExpression(stringValue);
        return ExpressionToColumnIdentVisitor.convert(expression);
    }

    private void assertIllegalArgumentExceptionOnConvert(String stringValue, String exceptionMessage) {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage(exceptionMessage);
        createIdentFromString(stringValue);
    }

    private void assertInvalidColumnNameExceptionOnConvert(String stringValue, String exceptionMessage) {
        expectedException.expect(InvalidColumnNameException.class);
        expectedException.expectMessage(exceptionMessage);
        createIdentFromString(stringValue);
    }

    private void assertExceptionOnUnsupportedConvert(String stringValue) {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Can't handle");
        createIdentFromString(stringValue);
    }
}
