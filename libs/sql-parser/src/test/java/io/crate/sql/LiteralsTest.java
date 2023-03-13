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


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.IntegerLiteral;

class LiteralsTest {

    @Test
    void testEscape() {
        assertThat(Literals.escapeStringLiteral("")).isEmpty();
        assertThat(Literals.escapeStringLiteral("foobar")).isEqualTo("foobar");
        assertThat(Literals.escapeStringLiteral("'")).isEqualTo("''");
        assertThat(Literals.escapeStringLiteral("''")).isEqualTo("''''");
        assertThat(Literals.escapeStringLiteral("'fooBar'")).isEqualTo("''fooBar''");
    }

    @Test
    void testQuote() {
        assertThat(Literals.quoteStringLiteral("")).isEqualTo("''");
        assertThat(Literals.quoteStringLiteral("foobar")).isEqualTo("'foobar'");
        assertThat(Literals.quoteStringLiteral("'")).isEqualTo("''''");
        assertThat(Literals.quoteStringLiteral("''")).isEqualTo("''''''");
        assertThat(Literals.quoteStringLiteral("'fooBar'")).isEqualTo("'''fooBar'''");
    }

    @Test
    void testThatNoEscapedCharsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("")).isEmpty();
        assertThat(Literals.replaceEscapedChars("Hello World")).isEqualTo("Hello World");
    }

    // Single escaped chars supported

    @Test
    void testThatEscapedTabLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\t")).isEqualTo("\t");
    }

    @Test
    void testThatEscapedTabInMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\tWorld")).isEqualTo("Hello\tWorld");
    }

    @Test
    void testThatEscapedTabAtBeginningOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\tHelloWorld")).isEqualTo("\tHelloWorld");
    }

    @Test
    void testThatEscapedTabAtEndOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("HelloWorld\\t")).isEqualTo("HelloWorld\t");
    }

    @Test
    void testThatEscapedBackspaceInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\bWorld")).isEqualTo("Hello\bWorld");
    }

    @Test
    void testThatEscapedFormFeedInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\fWorld")).isEqualTo("Hello\fWorld");
    }

    @Test
    void testThatEscapedNewLineInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\nWorld")).isEqualTo("Hello\nWorld");
    }

    @Test
    void testThatCarriageReturnInTheMiddleOfLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("Hello\\rWorld")).isEqualTo("Hello\rWorld");
    }

    @Test
    void testThatMultipleConsecutiveSingleEscapedCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\t\\n\\f")).isEqualTo("\t\n\f");
    }

    // Invalid escaped literals

    @Test
    void testThatCharEscapeWithoutAnySequenceIsNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\")).isEqualTo("\\");
    }

    @Test
    void testThatEscapedBackslashCharIsReplacedAndNotConsideredAsEscapeChar() {
        assertThat(Literals.replaceEscapedChars("\\\\141")).isEqualTo("\\141");
    }

    @Test
    void testThatEscapedQuoteCharIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\'141")).isEqualTo("'141");
    }

    @Test
    void testThatInvalidEscapeSequenceIsNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\s")).isEqualTo("\\s");
    }

    @Test
    void testThatInvalidEscapeSequenceAtBeginningOfLiteralIsNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\shello")).isEqualTo("\\shello");
    }

    @Test
    void testThatInvalidEscapeSequenceAtEndOfLiteralIsNotReplaced() {
        assertThat(Literals.replaceEscapedChars("hello\\s")).isEqualTo("hello\\s");
    }

    // Octal Byte Values

    @Test
    void testThatEscapedOctalValueLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\141")).isEqualTo("a");
    }

    @Test
    void testThatMultipleConsecutiveEscapedOctalValuesAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\141\\141\\141")).isEqualTo("aaa");
    }

    @Test
    void testThatDigitFollowingEscapedOctalValueIsNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\1411")).isEqualTo("a1");
    }

    @Test
    void testThatSingleDigitEscapedOctalValueIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\0")).isEqualTo("\u0000");
    }

    @Test
    void testThatDoubleDigitEscapedOctalValueIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\07")).isEqualTo("\u0007");
    }

    // Hexadecimal Byte Values

    @Test
    void testThatEscapedHexValueLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x61")).isEqualTo("a");
    }

    @Test
    void testThatMultipleConsecutiveEscapedHexValueLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x61\\x61\\x61")).isEqualTo("aaa");
    }

    @Test
    void testThatMultipleNonConsecutiveEscapedHexValueLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x61 \\x61")).isEqualTo("a a");
    }

    @Test
    void testThatDigitsFollowingEscapedHexValueLiteralAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x610000")).isEqualTo("a0000");
    }

    @Test
    void testThatSingleDigitEscapedHexValueLiteralIsReplaced() {
        assertThat(Literals.replaceEscapedChars("\\xDg0000")).isEqualTo("\rg0000");
    }

    @Test
    void testThatEscapedHexValueInTheMiddleOfTheLiteralAreReplacedIsReplaced() {
        assertThat(Literals.replaceEscapedChars("What \\x61 wonderful world"))
            .isEqualTo("What a wonderful world");
    }

    @Test
    void testThatInvalidEscapedHexLiteralsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\x\\x")).isEqualTo("xx");
    }

    // 16-bit Unicode Character Values

    @Test
    void testThatEscaped16BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u0061")).isEqualTo("a");
    }

    @Test
    void testThatMultipleConsecutiveEscaped16BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u0061\\u0061\\u0061")).isEqualTo("aaa");
    }

    @Test
    void testThatMultipleNonConsecutiveEscaped16BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u0061 \\u0061")).isEqualTo("a a");
    }

    @Test
    void testThatDigitsFollowingEscaped16BitUnicodeCharsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\u00610000")).isEqualTo("a0000");
    }

    @Test
    void testThatEscaped16BitUnicodeCharsInTheMiddleOfTheLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("What \\u0061 wonderful world"))
            .isEqualTo("What a wonderful world");
    }

    @Test
    void testThatInvalidLengthEscapedUnicode16SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\u006"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    void testThatInvalidHexEscapedUnicode16SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\u006G"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    void testThatEscaped32BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U00000061")).isEqualTo("a");
    }

    @Test
    void testThatMultipleConsecutiveEscaped32BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U00000061\\U00000061\\U00000061")).isEqualTo("aaa");
    }

    @Test
    void testThatMultipleNonConsecutiveEscaped32BitUnicodeCharsAreReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U00000061 \\U00000061")).isEqualTo("a a");
    }

    @Test
    void testThatDigitsFollowingEscaped32BitUnicodeCharsAreNotReplaced() {
        assertThat(Literals.replaceEscapedChars("\\U000000610000")).isEqualTo("a0000");
    }

    @Test
    void testThatEscaped32BitUnicodeCharsInTheMiddleOfTheLiteralAreReplaced() {
        assertThat(Literals.replaceEscapedChars("What \\U00000061 wonderful world"))
            .isEqualTo("What a wonderful world");
    }

    @Test
    void testThatInvalidLengthEscapedUnicode32SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\U0061"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    void testThatInvalidHexEscapedUnicode32SequenceThrowsException() {
        assertThatThrownBy(
            () -> Literals.replaceEscapedChars("\\U0000006G"))
            .isExactlyInstanceOf(IllegalArgumentException.class)
            .hasMessage(Literals.ESCAPED_UNICODE_ERROR);
    }

    @Test
    void test_integer_literal() {
        var literal = SqlParser.createExpression("2147483647");
        assertThat(literal).isEqualTo(new IntegerLiteral(Integer.MAX_VALUE));
    }
}
