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
import static io.crate.sql.tree.QueryUtil.selectList;
import static io.crate.sql.tree.QueryUtil.table;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.catchThrowableOfType;
import static org.assertj.core.api.Fail.fail;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;

import io.crate.common.collections.Lists;
import io.crate.sql.tree.ArrayLiteral;
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
import io.crate.sql.tree.SingleColumn;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.StringLiteral;

public class TestSqlParser {

    @Test
    public void testComments() {
        assertThat(
            SqlParser.createStatement("-- this is a line comment\nSelect 1"))
            .isExactlyInstanceOf(Query.class);
        assertThat(
            SqlParser.createStatement("Select 1\n-- this is a line comment"))
            .isExactlyInstanceOf(Query.class);
        assertThat(
            SqlParser.createStatement("-- this is a line comment\nSelect 1\n-- this is a line comment"))
            .isExactlyInstanceOf(Query.class);
        assertThat(
            SqlParser.createStatement("-- this is a line comment\nSelect \n-- this is a line comment\n1"))
            .isExactlyInstanceOf(Query.class);
        assertThat(SqlParser.createStatement(
                  """
                  /* this
                         is a multiline
                         comment
                      */
                  Select 1;"""))
            .isExactlyInstanceOf(Query.class);
        assertThat(SqlParser.createStatement(
                  """
                  Select 1;    /* this
                         is a multiline
                         comment
                      */"""))
            .isExactlyInstanceOf(Query.class);
        assertThat(SqlParser.createStatement(
                  """
                  Select    /* this
                         is a multiline
                         comment
                      */1"""))
            .isExactlyInstanceOf(Query.class);
        assertThat(SqlParser.createStatement(
                  """
                  Select    /* this
                         is a multiline
                         comment
                      */
                  -- line comment
                  1"""))
            .isExactlyInstanceOf(Query.class);
        assertThat(SqlParser.createStatement(
                """
                  CREATE TABLE IF NOT EXISTS "doc"."data" (
                     "week__generated" TIMESTAMP GENERATED ALWAYS AS date_trunc('week', "ts"),
                     "mid" STRING, -- measurement id, mainly used for triggers, starts for continuuous measurment with random uuid
                     "res" INTEGER, -- resolution in ms
                     "ts" TIMESTAMP,
                     "val_avg" FLOAT,
                     "val_max" FLOAT,
                     "val_min" FLOAT,
                     "val_stddev" FLOAT,
                     "vid" STRING, -- variable id, unique uuid
                     PRIMARY KEY ("ts", "mid", "vid", "res", "week__generated")
                  )
                  CLUSTERED INTO 3 SHARDS
                  PARTITIONED BY ("res", "week__generated")
                  WITH (
                     number_of_replicas = '1'
                  );"""))
            .isExactlyInstanceOf(CreateTable.class);
    }

    @Test
    public void testPossibleExponentialBacktracking() {
        SqlParser.createExpression("(((((((((((((((((((((((((((true)))))))))))))))))))))))))))");
    }

    @Test
    public void testDouble() {
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
            assertExpression(format(Locale.ENGLISH, "$%d", i), new ParameterExpression(i));
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
        assertThatThrownBy(
            () -> SqlParser.createExpression(""))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:1: mismatched input '<EOF>' expecting {");
    }

    @Test
    public void test_idents_starting_with_underscore() {
        var expected = new QualifiedNameReference(new QualifiedName(List.of("_", "_")));
        assertThat(SqlParser.createExpression("_._")).isEqualTo(expected);
        assertThat(SqlParser.createExpression("\"_\".\"_\"")).isEqualTo(expected);
    }

    @Test
    public void testEmptyStatement() {
        assertThatThrownBy(
            () -> SqlParser.createStatement(""))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:1: mismatched input '<EOF>' expecting");
    }

    @Test
    public void testExpressionWithTrailingJunk() {
        assertThatThrownBy(
            () -> SqlParser.createExpression("1 + 1 x"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:7: extraneous input 'x' expecting <EOF>");
    }

    @Test
    public void testTokenizeErrorStartOfLine() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("@select"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:1: extraneous input '@' expecting");
    }

    @Test
    public void testTokenizeErrorMiddleOfLine() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select * from foo where @what"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:25: no viable alternative at input 'select * from foo where @'");
    }

    @Test
    public void testTokenizeErrorIncompleteToken() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select * from 'oops"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:15: no viable alternative at input 'select * from ''");
    }

    @Test
    public void testParseErrorStartOfLine() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select *\nfrom x\nfrom"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 3:1: extraneous input 'from' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorMiddleOfLine() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select *\nfrom x\nwhere from"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 3:7: no viable alternative at input 'select *\\nfrom x\\nwhere from'");
    }

    @Test
    public void testParseErrorEndOfInput() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select * from"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:14: no viable alternative at input 'select * from'");
    }

    @Test
    public void testParseErrorEndOfInputWhitespace() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select * from  "))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:16: no viable alternative at input 'select * from  '");
    }

    @Test
    public void testParseErrorBackquotes() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select * from `foo`"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:15: backquoted identifiers are not supported; use double quotes to quote identifiers");
    }

    @Test
    public void testParseErrorBackquotesEndOfInput() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select * from foo `bar`"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:19: backquoted identifiers are not supported; use double quotes to quote identifiers");
    }

    @Test
    public void testParseErrorDigitIdentifiers() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select 1x from dual"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage(
                "line 1:8: identifiers must not start with a digit; surround the identifier with double quotes");
    }

    @Test
    public void testIdentifierWithColon() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select * from foo:bar"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:18: mismatched input ':' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorDualOrderBy() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual order by fuu order by fuu"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:35: mismatched input 'order' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorLimitAndFetch() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual limit 10 fetch first 5 rows only"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:31: mismatched input 'fetch' expecting {<EOF>, ';'}");
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual fetch next 10 row only limit 5"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:45: mismatched input 'limit' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorMultipleLimits() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual limit 1 limit 2 limit 3"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:30: mismatched input 'limit' expecting {<EOF>, ';'}");
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual fetch first 1 rows only fetch first 2 rows only"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:46: mismatched input 'fetch' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorReverseOrderByLimit() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual limit 10 order by fuu"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:31: mismatched input 'order' expecting {<EOF>, ';'}");
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual fetch first 10 rows only order by fuu"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:47: mismatched input 'order' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorReverseOrderByLimitOffset() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual limit 10 offset 20 order by fuu"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:41: mismatched input 'order' expecting {<EOF>, ';'}");
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual fetch first 10 row only offset 20 order by fuu"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:56: mismatched input 'order' expecting {<EOF>, ';'}");
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual offset ? fetch next 10 row only order by fuu"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:54: mismatched input 'order' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorReverseOrderByOffset() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual offset 20 order by fuu"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:32: mismatched input 'order' expecting {<EOF>, ';'}");
    }

    @Test
    public void testParseErrorMultipleOffsets() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("select fuu from dual offset 1 offset 2 offset 3"))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:31: mismatched input 'offset' expecting {<EOF>, ';'}");
    }


    @Test
    public void testParsingExceptionPositionInfo() {
        ParsingException pe = catchThrowableOfType(
            () -> SqlParser.createStatement("select *\nfrom x\nwhere from"), ParsingException.class);
        assertThat(pe.getMessage())
            .isEqualTo("line 3:7: no viable alternative at input 'select *\\nfrom x\\nwhere from'");
        assertThat(pe.getErrorMessage())
            .isEqualTo("no viable alternative at input 'select *\\nfrom x\\nwhere from'");
        assertThat(pe.getLineNumber()).isEqualTo(3);
        assertThat(pe.getColumnNumber()).isEqualTo(7);
    }

    @Test
    public void test_current_date_time_timestamp() {
        assertExpression("CURRENT_DATE", new CurrentTime(CurrentTime.Type.DATE));
        var stmt = SqlParser.createStatement("SELECT CURRENT_DATE AS CURRENT_DATE");
        assertThat(((QuerySpecification)((Query) stmt).getQueryBody()).getSelect().getSelectItems()).satisfiesExactly(
            s -> assertThat(((SingleColumn) s).getAlias()).isEqualTo("current_date")
        );

        assertExpression("CURRENT_TIME", new CurrentTime(CurrentTime.Type.TIME));
        stmt = SqlParser.createStatement("SELECT CURRENT_TIME CURRENT_TIME");
        assertThat(((QuerySpecification)((Query) stmt).getQueryBody()).getSelect().getSelectItems()).satisfiesExactly(
            s -> assertThat(((SingleColumn) s).getAlias()).isEqualTo("current_time")
        );

        assertExpression("CURRENT_TIMESTAMP", new CurrentTime(CurrentTime.Type.TIMESTAMP));
        stmt = SqlParser.createStatement("SELECT CURRENT_TIMESTAMP AS CURRENT_TIMESTAMP");
        assertThat(((QuerySpecification)((Query) stmt).getQueryBody()).getSelect().getSelectItems()).satisfiesExactly(
            s -> assertThat(((SingleColumn) s).getAlias()).isEqualTo("current_timestamp")
        );
    }

    @Test
    public void testCurrentSchemaFunction() {
        assertInstanceOf("CURRENT_SCHEMA", FunctionCall.class);
        assertInstanceOf("CURRENT_SCHEMA()", FunctionCall.class);
        var stmt = SqlParser.createStatement("SELECT CURRENT_SCHEMA AS CURRENT_SCHEMA");
        assertThat(((QuerySpecification)((Query) stmt).getQueryBody()).getSelect().getSelectItems()).satisfiesExactly(
            s -> assertThat(((SingleColumn) s).getAlias()).isEqualTo("current_schema")
        );
    }

    @Test
    public void testUserFunctions() {
        assertInstanceOf("CURRENT_USER", FunctionCall.class);
        assertInstanceOf("CURRENT_ROLE", FunctionCall.class);
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
        assertThatThrownBy(
            () -> assertInstanceOf("TRIM(' ' chars)", FunctionCall.class))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:10: no viable alternative at input 'TRIM(' ' chars'");
    }

    private void assertInstanceOf(String expr, Class<? extends Node> cls) {
        assertThat(SqlParser.createExpression(expr)).isExactlyInstanceOf(cls);
    }

    @Test
    public void testStackOverflowExpression() {
        assertThatThrownBy(
            () -> SqlParser.createExpression(Lists.joinOn(" OR ", nCopies(12000, "x = y"), x -> x)))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:1: expression is too large (stack overflow while parsing)");
    }

    @Test
    public void testStackOverflowStatement() {
        assertThatThrownBy(
            () -> SqlParser.createStatement("SELECT " + Lists.joinOn(" OR ", nCopies(12000, "x = y"), x -> x)))
            .isExactlyInstanceOf(ParsingException.class)
            .hasMessage("line 1:1: statement is too large (stack overflow while parsing)");
    }

    @Test
    public void testDataTypesWithWhitespaceCharacters() {
        Cast cast = (Cast) SqlParser.createExpression("1::double precision");
        assertThat(cast.getType()).isExactlyInstanceOf(ColumnType.class);
        assertThat(cast.getType().name()).isEqualTo("double precision");
    }

    @Test
    public void testFromStringLiteralCast() {
        assertInstanceOf("TIMESTAMP '2016-12-31 01:02:03.123'", Cast.class);
        assertInstanceOf("int2 '2016'", Cast.class);
    }

    @Test
    public void testFromStringLiteralCastDoesNotSupportArrayType() {
        assertThatThrownBy(
            () -> SqlParser.createExpression("array(boolean) '[1,2,0]'"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage(
                "type 'string' cast notation only supports primitive types. Use '::' or cast() operator instead.");
    }

    @Test
    public void testFromStringLiteralCastDoesNotSupportObjectType() {
        assertThatThrownBy(
            () -> SqlParser.createExpression("object '{\"x\": 10}'"))
            .isExactlyInstanceOf(UnsupportedOperationException.class)
            .hasMessage(
                "type 'string' cast notation only supports primitive types. Use '::' or cast() operator instead.");
    }

    @Test
    public void test_special_char_data_type() {
        Cast cast = (Cast) SqlParser.createExpression("1::\"char\"");
        assertThat(cast.getType().getClass()).isEqualTo(ColumnType.class);
        assertThat(cast.getType().name()).isEqualTo("\"char\"");
    }

    @Test
    public void test_dollar_quoted_strings_with_valid_tags() {
        assertThat(((StringLiteral) SqlParser.createExpression("$$$$")).getValue()).isEmpty();
        assertThat(((StringLiteral) SqlParser.createExpression("$a$ a$a $a$")).getValue()).isEqualTo(" a$a ");
        assertThat(((StringLiteral) SqlParser.createExpression("$a$$a$")).getValue()).isEmpty();
        assertThat(((StringLiteral) SqlParser.createExpression("$_$$_$")).getValue()).isEmpty();
        assertThat(((StringLiteral) SqlParser.createExpression("$_$string, content,\n!@#$_$")).getValue()).isEqualTo("string, content,\n!@#");
    }

    @Test
    public void test_comma_separated_dollar_quoted_strings() {
        // tests that '$$a$$, $$b$$' is not interpreted as 'a$$, $$b'
        var arrayLiteral = (ArrayLiteral) SqlParser.createExpression("[$$a$$, $$b,$$, $a$abc$a$]");
        var actual = arrayLiteral.values().stream().map(expr -> (StringLiteral) expr).map(StringLiteral::getValue).toList();
        assertThat(actual).isEqualTo(List.of("a", "b,", "abc"));
    }

    @Test
    public void test_dollar_quoted_positional_parameters() {
        assertThat(((StringLiteral) SqlParser.createExpression("$$$1$$")).getValue()).isEqualTo("$1");
        assertThat(((StringLiteral) SqlParser.createExpression("$$$1$2$3$$")).getValue()).isEqualTo("$1$2$3");
    }

    @Test
    public void test_dollar_quoted_strings_with_invalid_tags() {
        assertThatThrownBy(
            () -> SqlParser.createExpression("$a$a$a$a$")) // tokenize as $a$ | a | $a$ then the next 'a' is mismatched
            .isInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:8: mismatched input 'a' expecting {");
        assertThatThrownBy(
            () -> SqlParser.createExpression("$$$$$$"))
            .isInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:5: extraneous input '$$' expecting <EOF>");
        assertThatThrownBy(
            () -> SqlParser.createExpression("$1$$1$")) // $1$ is an invalid tag
            .isInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:3: mismatched input '$$' expecting {<EOF>, ");
        assertThatThrownBy(
            () -> SqlParser.createExpression("$a$$b$"))
            .isInstanceOf(ParsingException.class)
            .hasMessageStartingWith("line 1:7: extraneous input '<EOF>' expecting {$<tag>$<quoted string>$<tag>$, closing $<tag>$ for quoted string}");
    }

    @Test
    public void test_create_function_with_dollar_quoted_strings() {
        var createFunction = SqlParser.createStatement(
            """
                CREATE FUNCTION isbn_to_title(text) RETURNS text LANGUAGE sql
                AS $$SELECT title from books where isbn = $1$$;
                """
        );
        var expected = SqlParser.createStatement(
            """
                CREATE FUNCTION isbn_to_title(text) RETURNS text LANGUAGE sql
                AS 'SELECT title from books where isbn = $1';
                """
        );
        // CreateFunction.equals() does not compare the definitions
        assertThat(createFunction).hasToString(expected.toString());
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
