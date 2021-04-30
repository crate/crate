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

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import org.junit.Test;

public class EncodeDecodeFunctionTest extends ScalarTestCase {

    @Test
    public void testInvalidBytea() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal octal character b at index 4");
        assertEvaluate("encode('123\\b\\t56', 'base64')", null);
    }

    @Test
    public void testInvalidBased64() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal base64 character 8");
        assertEvaluate("decode(E'123\\b\\t56', 'base64')", null);
    }

    @Test
    public void testInvalidBinaryEncodeToBase64() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Illegal hexadecimal character h at index 3");
        assertEvaluate("encode('\\xfh', 'base64')", null);
    }

    @Test
    public void testInvalidBinaryEncodeToHex() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Illegal hexadecimal character h at index 3");
        assertEvaluate("encode('\\xfh', 'hex')", null);
    }

    @Test
    public void testInvalidBinaryEncodeToEscape() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Illegal hexadecimal character h at index 3");
        assertEvaluate("encode('\\xfh', 'escape')", null);
    }

    @Test
    public void testInvalidBinaryDecodeFromHex1() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Illegal hexadecimal character \\ at index 0");
        assertEvaluate("decode('\\xff', 'hex')", null);
    }

    @Test
    public void testInvalidBinaryDecodeFromHex2() {
        expectedException.expect(IllegalStateException.class);
        expectedException.expectMessage("Odd number of characters");
        assertEvaluate("decode('ffa', 'hex')", null);
    }

    @Test
    public void testNulls() {
        // null format
        assertEvaluate("encode('\\xff', null)", null);
        assertEvaluate("decode('FA==', null)", null);
        // null data
        assertEvaluate("encode(null, 'base64')", null);
        assertEvaluate("decode(null, 'base64')", null);
    }

    @Test
    public void testUnknownEncodeFormat() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Encoding format 'bad' is not supported");
        assertEvaluate("encode('\\xff', 'bad')", null);
    }

    @Test
    public void testUnknownDecodeFormat() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Encoding format 'bad' is not supported");
        assertEvaluate("decode('FA==', 'bad')", null);
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
