/*
 * Licensed to Crate.io Inc. or its affiliates ("Crate.io") under one or
 * more contributor license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding copyright ownership.
 * Crate.io licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the
 * License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * However, if you have executed another commercial license agreement with
 * Crate.io these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.parser;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.crate.sql.tree.*;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Locale;
import java.util.Optional;

import static io.crate.sql.SqlFormatter.formatSql;
import static io.crate.sql.tree.QueryUtil.selectList;
import static io.crate.sql.tree.QueryUtil.table;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestSqlParser {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testPossibleExponentialBacktracking()
        throws Exception {
        SqlParser.createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
    }

    @Test
    public void testDouble()
        throws Exception {
        assertExpression("123.", new DoubleLiteral("123"));
        assertExpression("123.0", new DoubleLiteral("123"));
        assertExpression(".5", new DoubleLiteral(".5"));
        assertExpression("123.5", new DoubleLiteral("123.5"));

        assertExpression("123E7", new DoubleLiteral("123E7"));
        assertExpression("123.E7", new DoubleLiteral("123E7"));
        assertExpression("123.0E7", new DoubleLiteral("123E7"));
        assertExpression("123E+7", new DoubleLiteral("123E7"));
        assertExpression("123E-7", new DoubleLiteral("123E-7"));

        assertExpression("123.456E7", new DoubleLiteral("123.456E7"));
        assertExpression("123.456E+7", new DoubleLiteral("123.456E7"));
        assertExpression("123.456E-7", new DoubleLiteral("123.456E-7"));

        assertExpression(".4E42", new DoubleLiteral(".4E42"));
        assertExpression(".4E+42", new DoubleLiteral(".4E42"));
        assertExpression(".4E-42", new DoubleLiteral(".4E-42"));
    }

    @Test
    public void testParameter() throws Exception {
        assertExpression("?", new ParameterExpression(1));
        for (int i = 0; i < 1000; i++) {
            assertExpression(String.format(Locale.ENGLISH, "$%d", i), new ParameterExpression(i));
        }
    }

    @Test
    public void testDoubleInQuery() {
        assertStatement("SELECT 123.456E7 FROM DUAL",
            new Query(
                Optional.empty(),
                new QuerySpecification(
                    selectList(new DoubleLiteral("123.456E7")),
                    table(QualifiedName.of("dual")),
                    Optional.empty(),
                    ImmutableList.of(),
                    Optional.empty(),
                    ImmutableList.of(),
                    Optional.empty(),
                    Optional.empty()),
                ImmutableList.of(),
                Optional.empty(),
                Optional.empty())
        );
    }

    @Test
    public void testEmptyExpression() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:1: no viable alternative at input '<EOF>'");
        SqlParser.createExpression("");
    }

    @Test
    public void testEmptyStatement() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:1: no viable alternative at input '<EOF>'");
        SqlParser.createStatement("");
    }

    @Test
    public void testExpressionWithTrailingJunk() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:7: extraneous input 'x' expecting");
        SqlParser.createExpression("1 + 1 x");
    }

    @Test
    public void testTokenizeErrorStartOfLine() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:1: extraneous input '@' expecting");
        SqlParser.createStatement("@select");
    }

    @Test
    public void testTokenizeErrorMiddleOfLine() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:25: no viable alternative at input '@'");
        SqlParser.createStatement("select * from foo where @what");
    }

    @Test
    public void testTokenizeErrorIncompleteToken() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:15: extraneous input ''' expecting");
        SqlParser.createStatement("select * from 'oops");
    }

    @Test
    public void testParseErrorStartOfLine() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 3:1: extraneous input 'from' expecting");
        SqlParser.createStatement("select *\nfrom x\nfrom");
    }

    @Test
    public void testParseErrorMiddleOfLine() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 3:7: no viable alternative at input 'from'");
        SqlParser.createStatement("select *\nfrom x\nwhere from");
    }

    @Test
    public void testParseErrorEndOfInput() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:14: no viable alternative at input '<EOF>'");
        SqlParser.createStatement("select * from");
    }

    @Test
    public void testParseErrorEndOfInputWhitespace() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:16: no viable alternative at input '<EOF>'");
        SqlParser.createStatement("select * from  ");
    }

    @Test
    public void testParseErrorBackquotes() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:15: backquoted identifiers are not supported; use double quotes to quote identifiers");
        SqlParser.createStatement("select * from `foo`");
    }

    @Test
    public void testParseErrorBackquotesEndOfInput() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:19: backquoted identifiers are not supported; use double quotes to quote identifiers");
        SqlParser.createStatement("select * from foo `bar`");
    }

    @Test
    public void testParseErrorDigitIdentifiers() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:8: identifiers must not start with a digit; surround the identifier with double quotes");
        SqlParser.createStatement("select 1x from dual");
    }

    @Test
    public void testIdentifierWithColon() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:15: identifiers must not contain ':'");
        SqlParser.createStatement("select * from foo:bar");
    }

    @Test
    public void testParseErrorDualOrderBy() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:35: mismatched input 'order'");
        SqlParser.createStatement("select fuu from dual order by fuu order by fuu");
    }

    @Test
    public void testParseErrorReverseOrderByLimit() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:31: mismatched input 'order' expecting <EOF>");
        SqlParser.createStatement("select fuu from dual limit 10 order by fuu");
    }

    @Test
    public void testParseErrorReverseOrderByLimitOffset() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:41: mismatched input 'order' expecting <EOF>");
        SqlParser.createStatement("select fuu from dual limit 10 offset 20 order by fuu");
    }

    @Test
    public void testParseErrorReverseOrderByOffset() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:32: mismatched input 'order' expecting <EOF>");
        SqlParser.createStatement("select fuu from dual offset 20 order by fuu");
    }

    @Test
    public void testParseErrorReverseLimitOffset() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:32: mismatched input 'limit' expecting <EOF>");
        SqlParser.createStatement("select fuu from dual offset 20 limit 10");
    }

    @Test
    public void testParsingExceptionPositionInfo() {
        try {
            SqlParser.createStatement("select *\nfrom x\nwhere from");
            fail("expected exception");
        } catch (ParsingException e) {
            assertEquals(e.getMessage(), "line 3:7: no viable alternative at input 'from'");
            assertEquals(e.getErrorMessage(), "no viable alternative at input 'from'");
            assertEquals(e.getLineNumber(), 3);
            assertEquals(e.getColumnNumber(), 7);
        }
    }

    @Test
    public void testDate() throws Exception {
        assertExpression("DATE '2012-03-22'", new DateLiteral("2012-03-22"));
    }

    @Test
    public void testTime() throws Exception {
        assertExpression("TIME '03:04:05'", new TimeLiteral("03:04:05"));
    }

    @Test
    public void testTimestamp() throws Exception {
        assertExpression("TIMESTAMP '2016-12-31 01:02:03.123'", new TimestampLiteral("2016-12-31 01:02:03.123"));
    }

    @Test
    public void testCurrentTimestamp()
        throws Exception {
        assertExpression("CURRENT_TIMESTAMP", new CurrentTime(CurrentTime.Type.TIMESTAMP));
    }

    @Test
    public void testCurrentSchemaFunction() throws Exception {
        assertInstanceOf("CURRENT_SCHEMA", FunctionCall.class);
        assertInstanceOf("CURRENT_SCHEMA()", FunctionCall.class);
    }

    @Test
    public void testUserFunctions() {
        assertInstanceOf("CURRENT_USER", FunctionCall.class);
        assertInstanceOf("SESSION_USER", FunctionCall.class);
        assertInstanceOf("USER", FunctionCall.class);
    }

    private void assertInstanceOf(String expr, Class<? extends Node> cls) {
        Expression expression = SqlParser.createExpression(expr);
        assertThat(expression, instanceOf(cls));
    }

    @Test
    public void testStackOverflowExpression() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:1: expression is too large (stack overflow while parsing)");
        SqlParser.createExpression(Joiner.on(" OR ").join(nCopies(4000, "x = y")));
    }

    @Test
    public void testStackOverflowStatement() {
        expectedException.expect(ParsingException.class);
        expectedException.expectMessage("line 1:1: statement is too large (stack overflow while parsing)");
        SqlParser.createStatement("SELECT " + Joiner.on(" OR ").join(nCopies(4000, "x = y")));
    }

    private static void assertStatement(String query, Statement expected) {
        assertParsed(query, expected, SqlParser.createStatement(query));
    }

    private static void assertExpression(String expression, Expression expected) {
        assertParsed(expression, expected, SqlParser.createExpression(expression));
    }

    private static void assertParsed(String input, Node expected, Node parsed) {
        if (!parsed.equals(expected)) {
            fail(format("expected%n%n%s%n%nto parse as%n%n%s%n%nbut was%n%n%s%n",
                indent(input),
                indent(formatSql(expected)),
                indent(formatSql(parsed))));
        }
    }

    private static String indent(String value) {
        String indent = "    ";
        return indent + value.trim().replaceAll("\n", "\n" + indent);
    }
}
