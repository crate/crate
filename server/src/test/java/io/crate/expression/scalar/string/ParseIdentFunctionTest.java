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

package io.crate.expression.scalar.string;

import static io.crate.testing.Asserts.isFunction;
import static io.crate.testing.Asserts.isLiteral;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.junit.Test;

import io.crate.exceptions.UnsupportedFunctionException;
import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;


public class ParseIdentFunctionTest extends ScalarTestCase {

    @Test
    public void testZeroArguments() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident()"))
            .isExactlyInstanceOf(UnsupportedFunctionException.class)
            .hasMessageContaining("Invalid arguments in: parse_ident()");
    }

    @Test
    public void testNullInput() {
        assertEvaluateNull("parse_ident(null)");
    }

    @Test
    public void testNullStrictMode() {
        assertEvaluateNull("parse_ident('foo', null)");
    }

    @Test
    public void testSimpleUnquotedIdentifier() {
        assertEvaluate("parse_ident('customers')", List.of("customers"));
    }

    @Test
    public void testUnquotedIdentifierCaseFolding() {
        assertEvaluate("parse_ident('SomeTable')", List.of("sometable"));
    }

    @Test
    public void testDotSeparatedUnquotedIdentifiers() {
        assertEvaluate("parse_ident('myschema.mytable')", List.of("myschema", "mytable"));
    }

    @Test
    public void testThreePartIdentifier() {
        assertEvaluate("parse_ident('John.Smith.Lily')", List.of("john", "smith", "lily"));
    }

    @Test
    public void testQuotedIdentifierPreservesCase() {
        assertEvaluate("parse_ident('\"SomeSchema\".sometable')", List.of("SomeSchema", "sometable"));
    }

    @Test
    public void testBothQuotedIdentifiers() {
        assertEvaluate("parse_ident('\"SomeSchema\".\"SomeTable\"')", List.of("SomeSchema", "SomeTable"));
    }

    @Test
    public void testQuotedIdentifierWithDot() {
        assertEvaluate("parse_ident('\"some.schema\".table1')", List.of("some.schema", "table1"));
    }

    @Test
    public void testQuotedIdentifierWithEscapedQuote() {
        assertEvaluate("parse_ident('\"foo\"\"bar\".baz')", List.of("foo\"bar", "baz"));
    }

    @Test
    public void testIdentifierWithUnderscore() {
        assertEvaluate("parse_ident('my_schema.my_table')", List.of("my_schema", "my_table"));
    }

    @Test
    public void testIdentifierStartingWithUnderscore() {
        assertEvaluate("parse_ident('_private')", List.of("_private"));
    }

    @Test
    public void testIdentifierWithDigits() {
        assertEvaluate("parse_ident('table1.col2')", List.of("table1", "col2"));
    }

    @Test
    public void testIdentifierWithDollarSignThrows() {
        // SqlParser does not support $ in unquoted identifiers
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('col$1')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testQuotedIdentifierWithDollarSign() {
        assertEvaluate("parse_ident('\"col$1\"')", List.of("col$1"));
    }

    @Test
    public void testWhitespaceTrimming() {
        assertEvaluate("parse_ident('  myschema . mytable  ')", List.of("myschema", "mytable"));
    }

    // Strict mode tests (default = true)

    @Test
    public void testStrictModeRejectsTrailingJunk() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('foo%%%')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testStrictModeExplicitTrue() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('foo%%%', true)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testStrictModeRejectsTrailingTextAfterIdentifier() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('John.Smith.Lily%%%')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    // Non-strict mode tests

    @Test
    public void testNonStrictModeIgnoresTrailingJunk() {
        assertEvaluate("parse_ident('John.Smith.Lily%%%', false)", List.of("john", "smith", "lily"));
    }

    @Test
    public void testNonStrictModeIgnoresTrailingChars() {
        assertEvaluate("parse_ident('foo()', false)", List.of("foo"));
    }

    @Test
    public void testNonStrictModeWithQuotedIdent() {
        assertEvaluate("parse_ident('\"SomeFunc\"(int)', false)", List.of("SomeFunc"));
    }

    // Error cases

    @Test
    public void testEmptyStringThrows() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testOnlyWhitespaceThrows() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('   ')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testTrailingDotThrows() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('schema.')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testStartsWithDigitThrows() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('1abc')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testUnclosedQuoteThrows() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('\"unclosed')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void testEmptyQuotedIdentifierThrows() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('\"\"')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    // Normalize and reference tests

    @Test
    public void testNormalizeWithLiteral() {
        assertNormalize("parse_ident('myschema.mytable')", isLiteral(List.of("myschema", "mytable")));
    }

    @Test
    public void testNormalizeWithRef() {
        assertNormalize(
            "parse_ident(name)",
            isFunction("parse_ident", List.of(DataTypes.STRING)));
    }

    @Test
    public void testEvaluateWithRef() {
        assertEvaluate(
            "parse_ident(name)", List.of("myschema", "mytable"),
            Literal.of(DataTypes.STRING, "myschema.mytable"));
    }

    @Test
    public void testEvaluateWithRefAndStrictMode() {
        assertEvaluate(
            "parse_ident(name, is_awesome)", List.of("foo"),
            Literal.of(DataTypes.STRING, "foo()"),
            Literal.of(DataTypes.BOOLEAN, false));
    }

    // PostgreSQL compatibility: the Grafana use-case from the issue
    @Test
    public void testSingleIdentifierArrayLength() {
        // parse_ident('customers') should return a 1-element array
        assertEvaluate("parse_ident('customers')", List.of("customers"));
    }

    @Test
    public void testSchemaQualifiedIdentifier() {
        // parse_ident('myschema.customers') should return a 2-element array
        assertEvaluate("parse_ident('myschema.customers')", List.of("myschema", "customers"));
    }

    @Test
    public void testQuotedUnicodeLetterIdentifier() {
        assertEvaluate("parse_ident('\"tëst\"')", List.of("tëst"));
    }

    @Test
    public void test_parse_ident_quoted_with_spaces() {
        assertEvaluate("parse_ident('\"my schema\".\"my table\"')", List.of("my schema", "my table"));
    }

    @Test
    public void test_parse_ident_single_quoted_ident_preserved() {
        assertEvaluate("parse_ident('\"UPPER\"')", List.of("UPPER"));
    }

    // Non-strict mode: qname fallback edge cases

    @Test
    public void test_non_strict_trailing_space_and_word() {
        assertEvaluate("parse_ident('foo.bar baz', false)", List.of("foo", "bar"));
    }

    @Test
    public void test_non_strict_quoted_ident_with_trailing_junk() {
        assertEvaluate("parse_ident('\"MySchema\".\"MyTable\"%%%', false)", List.of("MySchema", "MyTable"));
    }

    @Test
    public void test_non_strict_single_ident_trailing_operators() {
        assertEvaluate("parse_ident('foo%%%', false)", List.of("foo"));
    }

    @Test
    public void test_non_strict_only_junk_throws() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('%%%', false)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void test_non_strict_empty_string_throws() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('', false)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void test_non_strict_whitespace_only_throws() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('   ', false)"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void test_strict_trailing_space_and_word_throws() {
        assertThatThrownBy(() -> assertEvaluateNull("parse_ident('foo.bar baz')"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining("String is not a valid identifier");
    }

    @Test
    public void test_non_strict_non_reserved_keyword_as_ident() {
        // Non-reserved keywords like 'analyzer' are valid as identifiers
        assertEvaluate("parse_ident('analyzer.alias', false)", List.of("analyzer", "alias"));
    }
}
