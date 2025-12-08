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

package io.crate.expression.scalar;

import static io.crate.testing.Asserts.isFunction;

import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

public class LengthFunctionTest extends ScalarTestCase {

    @Test
    public void testOctetLengthEvaluateOnString() throws Exception {
        assertEvaluate("octet_length('©rate')", 6);
        assertEvaluate("octet_length('crate')", 5);
        assertEvaluate("octet_length('')", 0);
    }

    @Test
    public void testBitLengthEvaluateOnString() throws Exception {
        assertEvaluate("bit_length('©rate')", 48);
        assertEvaluate("bit_length('crate')", 40);
        assertEvaluate("bit_length('')", 0);
    }

    @Test
    public void testCharLengthEvaluateOnString() throws Exception {
        assertEvaluate("char_length('©rate')", 5);
        assertEvaluate("char_length('crate')", 5);
        assertEvaluate("char_length('')", 0);
    }

    @Test
    public void testOctetLengthEvaluateOnNull() throws Exception {
        assertEvaluateNull("octet_length(null)");
        assertEvaluateNull("octet_length(name)", Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testBitLengthEvaluateOnNull() throws Exception {
        assertEvaluateNull("bit_length(null)");
        assertEvaluateNull("bit_length(name)", Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testCharLengthEvaluateOnNull() throws Exception {
        assertEvaluateNull("char_length(null)");
        assertEvaluateNull("char_length(name)", Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("bit_length(name)", isFunction("bit_length"));
        assertEvaluate("bit_length(name)", 16, Literal.of("©"));
        assertEvaluate("octet_length(name)", 2, Literal.of("©"));
        assertEvaluate("char_length(name)", 1, Literal.of("©"));
    }

    @Test
    public void test_length_returns_number_of_characters_in_string() {
        assertEvaluate("length('')", 0);
        assertEvaluate("length('cra')", 3);
        assertEvaluate("length('©rate')", 5);
        assertEvaluate("length('🥝')", 1);
    }

    @Test
    public void test_length_returns_null_for_null_input() {
        assertEvaluateNull("length(null)");
    }
}
