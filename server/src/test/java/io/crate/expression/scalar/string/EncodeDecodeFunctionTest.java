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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;

public class EncodeDecodeFunctionTest extends ScalarTestCase {

    @Test
    public void testInvalidBytea() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("encode('123\\b\\t56', 'base64')");

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Illegal octal character b at index 4");
    }

    @Test
    public void testInvalidBased64() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("decode(E'123\\b\\t56', 'base64')");

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Illegal base64 character 8");
    }

    @Test
    public void testInvalidBinaryEncodeToBase64() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("encode('\\xfh', 'base64')");

        })
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Illegal hexadecimal character h at index 3");
    }

    @Test
    public void testInvalidBinaryEncodeToHex() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("encode('\\xfh', 'hex')");

        })
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Illegal hexadecimal character h at index 3");
    }

    @Test
    public void testInvalidBinaryEncodeToEscape() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("encode('\\xfh', 'escape')");

        })
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Illegal hexadecimal character h at index 3");
    }

    @Test
    public void testInvalidBinaryDecodeFromHex1() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("decode('\\xff', 'hex')");

        })
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Illegal hexadecimal character \\ at index 0");
    }

    @Test
    public void testInvalidBinaryDecodeFromHex2() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("decode('ffa', 'hex')");

        })
                .isExactlyInstanceOf(IllegalStateException.class)
                .hasMessage("Odd number of characters");
    }

    @Test
    public void testNulls() {
        // null format
        assertEvaluateNull("encode('\\xff', null)");
        assertEvaluateNull("decode('FA==', null)");
        // null data
        assertEvaluateNull("encode(null, 'base64')");
        assertEvaluateNull("decode(null, 'base64')");
    }

    @Test
    public void testUnknownEncodeFormat() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("encode('\\xff', 'bad')");

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Encoding format 'bad' is not supported");
    }

    @Test
    public void testUnknownDecodeFormat() {
        assertThatThrownBy(() -> {
            assertEvaluateNull("decode('FA==', 'bad')");

        })
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage("Encoding format 'bad' is not supported");
    }

    @Test
    public void testEncodeFuncBase64() {
        // input in hex format
        final Literal<String> name = Literal.of("\\x3132330001");
        assertEvaluate("encode('\\x3132330001', 'base64')", "MTIzAAE=");
        assertEvaluate("encode(name, 'Base64')", "MTIzAAE=", name);
        // input in escape format
        assertEvaluate("encode('123\\000\\001', 'base64')", "MTIzAAE=");
        assertEvaluate("encode('123', 'base64')", "MTIz");
    }

    @Test
    public void testDecodeFuncBase64() {
        Literal<String> name = Literal.of("MTIzAAE=");
        assertEvaluate("decode('MTIzAAE=', 'base64')", "\\x3132330001");
        assertEvaluate("decode('MTIzAAE=', 'BASE64')", "\\x3132330001");
        assertEvaluate("decode(name, 'base64')", "\\x3132330001", name);
    }

    @Test
    public void testEncodeFuncHex() {
        assertEvaluate("encode('\\x3132330001', 'hex')", "3132330001");
        assertEvaluate("encode('123\\000\\001', 'hex')", "3132330001");
    }

    @Test
    public void testDecodeFuncHex() {
        assertEvaluate("decode('3132330001', 'hex')", "\\x3132330001");
    }

    @Test
    public void testEncodeEmpties() {
        assertEvaluate("encode('', 'base64')", "");
        assertEvaluate("encode('', 'hex')", "");
        assertEvaluate("encode('', 'escape')", "");
    }

    @Test
    public void testEncodeFuncEscape() {
        assertEvaluate("encode('a\bb\\c', 'escape')", "a\\010b\\\\c");
        assertEvaluate("encode('\\x6108625c63', 'escape')", "a\\010b\\\\c");
    }

    @Test
    public void testDecodeFuncEscape() {
        assertEvaluate("decode('a\\010b\\\\c', 'escape')", "\\x6108625c63");
    }

}
