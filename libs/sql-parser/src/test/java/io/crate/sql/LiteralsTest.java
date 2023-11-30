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

package io.crate.sql;


import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.IntegerLiteral;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class LiteralsTest {

    @Test
    public void testEscape() {
        assertThat(Literals.escapeStringLiteral("")).isEqualTo("");
        assertThat(Literals.escapeStringLiteral("foobar")).isEqualTo("foobar");
        assertThat(Literals.escapeStringLiteral("'")).isEqualTo("''");
        assertThat(Literals.escapeStringLiteral("''")).isEqualTo("''''");
        assertThat(Literals.escapeStringLiteral("'fooBar'")).isEqualTo("''fooBar''");
    }

    @Test
    public void testQuote() {
        assertThat(Literals.quoteStringLiteral("")).isEqualTo("''");
        assertThat(Literals.quoteStringLiteral("foobar")).isEqualTo("'foobar'");
        assertThat(Literals.quoteStringLiteral("'")).isEqualTo("''''");
        assertThat(Literals.quoteStringLiteral("''")).isEqualTo("''''''");
        assertThat(Literals.quoteStringLiteral("'fooBar'")).isEqualTo("'''fooBar'''");
    }

    @Test
    public void testThatNoEscapedCharsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("")).isEqualTo("");
        assertThat(Literals.replaceEscapedChars("Hello World")).isEqualTo("Hello World");
    }

    // Single escaped chars supported

    @Test
    public void testThatEscapedTabLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\t")).isEqualTo("\t");
    }

    @Test
    public void testThatEscapedTabInMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\tWorld")).isEqualTo("Hello\tWorld");
    }

    @Test
    public void testThatEscapedTabAtBeginningOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\tHelloWorld")).isEqualTo("\tHelloWorld");
    }

    @Test
    public void testThatEscapedTabAtEndOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("HelloWorld\\t")).isEqualTo("HelloWorld\t");
    }

    @Test
    public void testThatEscapedBackspaceInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\bWorld")).isEqualTo("Hello\bWorld");
    }

    @Test
    public void testThatEscapedFormFeedInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\fWorld")).isEqualTo("Hello\fWorld");
    }

    @Test
    public void testThatEscapedNewLineInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\nWorld")).isEqualTo("Hello\nWorld");
    }

    @Test
    public void testThatCarriageReturnInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\rWorld")).isEqualTo("Hello\rWorld");
    }

    @Test
    public void testThatMultipleConsecutiveSingleEscapedCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\t\\n\\f")).isEqualTo("\t\n\f");
    }

    @Test
    public void test_next_symbol_after_backslash_is_taken_literally() {
        assertThat(Literals.replaceEscapedChars("ab\\%cde")).isEqualTo("ab%cde");
    }

    // Invalid escaped literals

    @Test
    public void testThatCharEscapeWithoutAnySequenceIsNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\")).isEqualTo("\\");
    }

    @Test
    public void testThatEscapedBackslashCharIsReplacedAndNotConsideredAsEscapeChar() {
        assertThat(Literals.replaceEscapedChars("\\\\141")).isEqualTo("\\141");
    }

    @Test
    public void testThatEscapedQuoteCharIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\'141")).isEqualTo("'141");
    }

    @Test
    public void test_next_symbol_after_backslash_at_the_beginning_is_taken_literally() {
        assertThat(Literals.replaceEscapedChars("\\shello")).isEqualTo("shello");
    }

    @Test
    public void test_next_symbol_after_backslash_at_the_endis_taken_literally() {
        assertThat(Literals.replaceEscapedChars("hello\\s")).isEqualTo("hellos");
    }

    // Octal Byte Values

    @Test
    public void testThatEscapedOctalValueLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\141")).isEqualTo("a");
    }

    @Test
    public void testThatMultipleConsecutiveEscapedOctalValuesAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\141\\141\\141")).isEqualTo("aaa");
    }

    @Test
    public void testThatDigitFollowingEscapedOctalValueIsNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\1411")).isEqualTo("a1");
    }

    @Test
    public void testThatSingleDigitEscapedOctalValueIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\0")).isEqualTo("\u0000");
    }

    @Test
    public void testThatDoubleDigitEscapedOctalValueIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\07")).isEqualTo("\u0007");
    }

    // Hexadecimal Byte Values

    @Test
    public void testThatEscapedHexValueLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x61")).isEqualTo("a");
    }

    @Test
    public void testThatMultipleConsecutiveEscapedHexValueLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x61\\x61\\x61")).isEqualTo("aaa");
    }

    @Test
    public void testThatMultipleNonConsecutiveEscapedHexValueLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x61 \\x61")).isEqualTo("a a");
    }

    @Test
    public void testThatDigitsFollowingEscapedHexValueLiteralAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x610000")).isEqualTo("a0000");
    }

    @Test
    public void testThatSingleDigitEscapedHexValueLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\xDg0000")).isEqualTo("\rg0000");
    }

    @Test
    public void testThatEscapedHexValueInTheMiddleOfTheLiteralAreReplacedIsReplaced() {
        assertThat(Literals.replaceEscapedChars("What \\x61 wonderful world"))
            .isEqualTo("What a wonderful world");
    }

    @Test
    public void testThatInvalidEscapedHexLiteralsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x\\x")).isEqualTo("xx");
    }

    // 16-bit Unicode Character Values

    @Test
    public void testThatEscaped16BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u0061")).isEqualTo("a");
    }

    @Test
    public void testThatMultipleConsecutiveEscaped16BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u0061\\u0061\\u0061")).isEqualTo("aaa");
    }

    @Test
    public void testThatMultipleNonConsecutiveEscaped16BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u0061 \\u0061")).isEqualTo("a a");
    }

    @Test
    public void testThatDigitsFollowingEscaped16BitUnicodeCharsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u00610000")).isEqualTo("a0000");
    }

    @Test
    public void testThatEscaped16BitUnicodeCharsInTheMiddleOfTheLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("What \\u0061 wonderful world"))
            .isEqualTo("What a wonderful world");
    }

    @Test
    public void testThatInvalidLengthEscapedUnicode16SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\u006"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    public void testThatInvalidHexEscapedUnicode16SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\u006G"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    public void testThatEscaped32BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U00000061")).isEqualTo("a");
    }

    @Test
    public void testThatMultipleConsecutiveEscaped32BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U00000061\\U00000061\\U00000061")).isEqualTo("aaa");
    }

    @Test
    public void testThatMultipleNonConsecutiveEscaped32BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U00000061 \\U00000061")).isEqualTo("a a");
    }

    @Test
    public void testThatDigitsFollowingEscaped32BitUnicodeCharsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U000000610000")).isEqualTo("a0000");
    }

    @Test
    public void testThatEscaped32BitUnicodeCharsInTheMiddleOfTheLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("What \\U00000061 wonderful world"))
            .isEqualTo("What a wonderful world");
    }

    @Test
    public void testThatInvalidLengthEscapedUnicode32SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\U0061"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    public void testThatInvalidHexEscapedUnicode32SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\U0000006G"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    public void test_integer_literal() {
        var literal = SqlParser.createExpression("2147483647");
        assertThat(literal).isEqualTo(new IntegerLiteral(Integer.MAX_VALUE));
    }
}
