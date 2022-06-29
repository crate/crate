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

package io.crate.sql.parser;

import static io.crate.sql.SqlFormatter.formatSql;
import static io.crate.sql.testing.Asserts.assertThrowsMatches;
import static io.crate.sql.tree.QueryUtil.selectList;
import static io.crate.sql.tree.QueryUtil.table;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import io.crate.common.collections.Lists2;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CurrentTime;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.Query;
import io.crate.sql.tree.QuerySpecification;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.StringLiteral;

public class TestSqlParser {

    @Test
    public void testComments() {
        assertThat(
            SqlParser.createStatement("-- this is a line comment\nSelect 1"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("Select 1\n-- this is a line comment"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("-- this is a line comment\nSelect 1\n-- this is a line comment"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("-- this is a line comment\nSelect \n-- this is a line comment\n1"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("/* this\n" +
                                  "       is a multiline\n" +
                                  "       comment\n" +
                                  "    */\nSelect 1;"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("Select 1;" +
                                  "    /* this\n" +
                                  "       is a multiline\n" +
                                  "       comment\n" +
                                  "    */"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("Select" +
                                             "    /* this\n" +
                                             "       is a multiline\n" +
                                             "       comment\n" +
                                             "    */" +
                                             "1"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("Select" +
                                      "    /* this\n" +
                                      "       is a multiline\n" +
                                      "       comment\n" +
                                      "    */\n" +
                                      "-- line comment\n" +
                                      "1"),
            instanceOf(Query.class));
        assertThat(
            SqlParser.createStatement("CREATE TABLE IF NOT EXISTS \"doc\".\"data\" (\n" +
                                      "   \"week__generated\" TIMESTAMP GENERATED ALWAYS AS date_trunc('week', \"ts\"),\n" +
                                      "   \"mid\" STRING, -- measurement id, mainly used for triggers, starts for continuuous measurment with random uuid\n" +
                                      "   \"res\" INTEGER, -- resolution in ms\n" +
                                      "   \"ts\" TIMESTAMP,\n" +
                                      "   \"val_avg\" FLOAT,\n" +
                                      "   \"val_max\" FLOAT,\n" +
                                      "   \"val_min\" FLOAT,\n" +
                                      "   \"val_stddev\" FLOAT,\n" +
                                      "   \"vid\" STRING, -- variable id, unique uuid\n" +
                                      "   PRIMARY KEY (\"ts\", \"mid\", \"vid\", \"res\", \"week__generated\")\n" +
                                      ")\n" +
                                      "CLUSTERED INTO 3 SHARDS\n" +
                                      "PARTITIONED BY (\"res\", \"week__generated\")\n" +
                                      "WITH (\n" +
                                      "   number_of_replicas = '1'\n" +
                                      ");"),
                instanceOf(CreateTable.class));
    }

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
    public void testParameter() {
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
                    List.of(),
                    Optional.empty(),
                    Map.of(),
                    List.of(),
                    Optional.empty(),
                    Optional.empty()),
                List.of(),
                Optional.empty(),
                Optional.empty())
        );
    }

    @Test
    public void testEmptyExpression() {
        assertThrowsMatches(
            () -> SqlParser.createExpression(""),
            ParsingException.class,
            "line 1:1: mismatched input '<EOF>'");
    }

    @Test
    public void testEmptyStatement() {
        assertThrowsMatches(
            () -> SqlParser.createStatement(""),
            ParsingException.class,
            "line 1:1: mismatched input '<EOF>'");
    }

    @Test
    public void testExpressionWithTrailingJunk() {
        assertThrowsMatches(
            () -> SqlParser.createExpression("1 + 1 x"),
            ParsingException.class,
            "line 1:7: extraneous input 'x' expecting");
    }

    @Test
    public void testTokenizeErrorStartOfLine() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("@select"),
            ParsingException.class,
            "line 1:1: extraneous input '@' expecting");
    }

    @Test
    public void testTokenizeErrorMiddleOfLine() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select * from foo where @what"),
            ParsingException.class,
            "line 1:25: no viable alternative at input 'select * from foo where @'");
    }

    @Test
    public void testTokenizeErrorIncompleteToken() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select * from 'oops"),
            ParsingException.class,
            "line 1:15: no viable alternative at input 'select * from ''");
    }

    @Test
    public void testParseErrorStartOfLine() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select *\nfrom x\nfrom"),
            ParsingException.class,
            "line 3:1: extraneous input 'from' expecting");
    }

    @Test
    public void testParseErrorMiddleOfLine() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select *\nfrom x\nwhere from"),
            ParsingException.class,
            "line 3:7: no viable alternative at input 'select *\\nfrom x\\nwhere from'");
    }

    @Test
    public void testParseErrorEndOfInput() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select * from"),
            ParsingException.class,
            "line 1:14: no viable alternative at input 'select * from'");
    }

    @Test
    public void testParseErrorEndOfInputWhitespace() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select * from  "),
            ParsingException.class,
            "line 1:16: no viable alternative at input 'select * from  '");
    }

    @Test
    public void testParseErrorBackquotes() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select * from `foo`"),
            ParsingException.class,
            "line 1:15: backquoted identifiers are not supported; use double quotes to quote identifiers");
    }

    @Test
    public void testParseErrorBackquotesEndOfInput() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select * from foo `bar`"),
            ParsingException.class,
            "line 1:19: backquoted identifiers are not supported; use double quotes to quote identifiers");
    }

    @Test
    public void testParseErrorDigitIdentifiers() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select 1x from dual"),
            ParsingException.class,
            "line 1:8: identifiers must not start with a digit; surround the identifier with double quotes");
    }

    @Test
    public void testIdentifierWithColon() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select * from foo:bar"),
            ParsingException.class,
            "line 1:18: mismatched input ':' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorDualOrderBy() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual order by fuu order by fuu"),
            ParsingException.class,
            "line 1:35: mismatched input 'order'");
    }

    @Test
    public void testParseErrorLimitAndFetch() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual limit 10 fetch first 5 rows only"),
            ParsingException.class,
            "line 1:31: mismatched input 'fetch' expecting {<EOF>, ';'}");
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual fetch next 10 row only limit 5"),
            ParsingException.class,
            "line 1:45: mismatched input 'limit' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorMultipleLimits() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual limit 1 limit 2 limit 3"),
            ParsingException.class,
            "line 1:30: mismatched input 'limit' expecting {<EOF>, ';'}");
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual fetch first 1 rows only fetch first 2 rows only"),
            ParsingException.class,
            "line 1:46: mismatched input 'fetch' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorReverseOrderByLimit() {
        assertThrowsMatches(
             () -> SqlParser.createStatement("select fuu from dual limit 10 order by fuu"),
             ParsingException.class,
             "line 1:31: mismatched input 'order' expecting {<EOF>, ';'}");
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual fetch first 10 rows only order by fuu"),
            ParsingException.class,
            "line 1:47: mismatched input 'order' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorReverseOrderByLimitOffset() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual limit 10 offset 20 order by fuu"),
            ParsingException.class,
            "line 1:41: mismatched input 'order' expecting {<EOF>, ';'}");
        assertThrowsMatches(
             () -> SqlParser.createStatement("select fuu from dual fetch first 10 row only offset 20 order by fuu"),
             ParsingException.class,
             "line 1:56: mismatched input 'order' expecting {<EOF>, ';'}");
        assertThrowsMatches(
             () -> SqlParser.createStatement("select fuu from dual offset ? fetch next 10 row only order by fuu"),
             ParsingException.class,
             "line 1:54: mismatched input 'order' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorReverseOrderByOffset() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual offset 20 order by fuu"),
            ParsingException.class,
            "line 1:32: mismatched input 'order' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorMultipleOffsets() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("select fuu from dual offset 1 offset 2 offset 3"),
            ParsingException.class,
            "line 1:31: mismatched input 'offset' expecting {<EOF>, ';'}");
    }


    @Test
    public void testParsingExceptionPositionInfo() {
        try {
            SqlParser.createStatement("select *\nfrom x\nwhere from");
            fail("expected exception");
        } catch (ParsingException e) {
            assertEquals(e.getMessage(), "line 3:7: no viable alternative at input 'select *\\nfrom x\\nwhere from'");
            assertEquals(e.getErrorMessage(), "no viable alternative at input 'select *\\nfrom x\\nwhere from'");
            assertEquals(e.getLineNumber(), 3);
            assertEquals(e.getColumnNumber(), 7);
        }
    }

    @Test
    public void testCurrentTimestamp() {
        assertExpression("CURRENT_TIMESTAMP", new CurrentTime(CurrentTime.Type.TIMESTAMP));
    }

    @Test
    public void testCurrentSchemaFunction() {
        assertInstanceOf("CURRENT_SCHEMA", FunctionCall.class);
        assertInstanceOf("CURRENT_SCHEMA()", FunctionCall.class);
    }

    @Test
    public void testUserFunctions() {
        assertInstanceOf("CURRENT_USER", FunctionCall.class);
        assertInstanceOf("SESSION_USER", FunctionCall.class);
        assertInstanceOf("USER", FunctionCall.class);
    }

    @Test
    public void testTrimFunctionExpression() {
        assertExpression("TRIM(BOTH 'A' FROM chars)",
            new FunctionCall(new QualifiedName("trim"), List.of(
                new QualifiedNameReference(new QualifiedName("chars")),
                new StringLiteral("A"),
                new StringLiteral("BOTH")
            ))
        );
    }

    @Test
    public void testTrimFunctionExpressionSingleArgument() {
        assertExpression("TRIM(chars)",
            new FunctionCall(new QualifiedName("trim"), List.of(
                new QualifiedNameReference(new QualifiedName("chars"))
            ))
        );
    }

    @Test
    public void testTrimFunctionAllArgs() {
        assertInstanceOf("TRIM(LEADING 'A' FROM chars)", FunctionCall.class);
        assertInstanceOf("TRIM(TRAILING 'A' FROM chars)", FunctionCall.class);
        assertInstanceOf("TRIM(BOTH 'A' FROM chars)", FunctionCall.class);
    }

    @Test
    public void testTrimFunctionDefaultTrimModeOnly() {
        assertInstanceOf("TRIM('A' FROM chars)", FunctionCall.class);
    }

    @Test
    public void testTrimFunctionDefaultCharsToTrimOnly() {
        assertInstanceOf("TRIM(LEADING FROM chars)", FunctionCall.class);
        assertInstanceOf("TRIM(TRAILING FROM chars)", FunctionCall.class);
        assertInstanceOf("TRIM(BOTH FROM chars)", FunctionCall.class);
        assertInstanceOf("TRIM(FROM chars)", FunctionCall.class);
    }

    @Test
    public void testTrimFunctionDefaultTrimModeAndCharsToTrim() {
        assertInstanceOf("TRIM(chars)", FunctionCall.class);
    }

    @Test
    public void testTrimFunctionMissingFromWhenCharsToTrimIsPresentThrowsException() {
        assertThrowsMatches(
            () -> assertInstanceOf("TRIM(' ' chars)", FunctionCall.class),
            ParsingException.class,
            "line 1:10: no viable alternative at input 'TRIM(' ' chars'");
    }

    private void assertInstanceOf(String expr, Class<? extends Node> cls) {
        Expression expression = SqlParser.createExpression(expr);
        assertThat(expression, instanceOf(cls));
    }

    @Test
    public void testStackOverflowExpression() {
        assertThrowsMatches(
            () -> SqlParser.createExpression(Lists2.joinOn(" OR ", nCopies(4000, "x = y"), x -> x)),
            ParsingException.class,
            "line 1:1: expression is too large (stack overflow while parsing)");
    }

    @Test
    public void testStackOverflowStatement() {
        assertThrowsMatches(
            () -> SqlParser.createStatement("SELECT " + Lists2.joinOn(" OR ", nCopies(4000, "x = y"), x -> x)),
            ParsingException.class,
            "line 1:1: statement is too large (stack overflow while parsing)");
    }

    @Test
    public void testDataTypesWithWhitespaceCharacters() {
        Cast cast = (Cast) SqlParser.createExpression("1::double precision");
        assertThat(cast.getType().getClass(), is(ColumnType.class));
        assertThat(cast.getType().name(), is("double precision"));
    }

    @Test
    public void testFromStringLiteralCast() {
        assertInstanceOf("TIMESTAMP '2016-12-31 01:02:03.123'", Cast.class);
        assertInstanceOf("int2 '2016'", Cast.class);
    }

    @Test
    public void testFromStringLiteralCastDoesNotSupportArrayType() {
        assertThrowsMatches(
            () -> SqlParser.createExpression("array(boolean) '[1,2,0]'"),
            UnsupportedOperationException.class,
            "type 'string' cast notation only supports primitive types. Use '::' or cast() operator instead.");
    }

    @Test
    public void testFromStringLiteralCastDoesNotSupportObjectType() {
        assertThrowsMatches(
            () -> SqlParser.createExpression("object '{\"x\": 10}'"),
            UnsupportedOperationException.class,
            "type 'string' cast notation only supports primitive types. Use '::' or cast() operator instead.");
    }

    @Test
    public void test_special_char_data_type() {
        Cast cast = (Cast) SqlParser.createExpression("1::\"char\"");
        assertThat(cast.getType().getClass(), is(ColumnType.class));
        assertThat(cast.getType().name(), is("\"char\""));
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
