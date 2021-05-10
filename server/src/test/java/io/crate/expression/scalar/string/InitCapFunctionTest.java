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
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class InitCapFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeCapInitFuncForAllLowerCase() {
        assertNormalize("initcap('hello world!')", isLiteral("Hello World!"));
    }

    @Test
    public void testNormalizeCapInitFuncForAllUpperCase() {
        assertNormalize("initcap('HELLO WORLD!')", isLiteral("Hello World!"));
    }

    @Test
    public void testNormalizeCapInitFuncForMixedCase() {
        assertNormalize("initcap('HellO 1WORLD !')", isLiteral("Hello 1world !"));
    }

    @Test
    public void testNormalizeCapInitFuncForEmptyString() {
        assertNormalize("initcap('')", isLiteral(""));
    }

    @Test
    public void testNormalizeCapInitFuncForNonEnglishLatinChars() {
        assertNormalize("initcap('ÄÖÜ αß àbc γ')", isLiteral("Äöü Αß Àbc Γ"));
    }

    @Test
    public void testNormalizeCapInitFuncForNonLatinChars() {
        assertNormalize("initcap('汉字 this is chinese!')", isLiteral("汉字 This Is Chinese!"));
    }

    @Test
    public void testEvaluateWithNull() {
        assertEvaluate("initcap(name)", null, Literal.of(DataTypes.STRING, null));
    }
}
