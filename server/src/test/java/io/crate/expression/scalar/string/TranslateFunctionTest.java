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
import org.junit.Test;

import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class TranslateFunctionTest extends ScalarTestCase {

    @Test
    public void testNormalizeTranslateFunc() throws Exception {
        assertNormalize("translate('Crate', 'Ct', 'Dk')", isLiteral("Drake"));
    }

    @Test
    public void testEvaluateTranslate() throws Exception {
        assertEvaluate("translate(name, 'Ct', 'Dk')", "Drake", Literal.of(DataTypes.STRING, "Crate"));
        assertEvaluate("translate('time', name, name)", "Zeit",
                       Literal.of(DataTypes.STRING, "emit"), Literal.of(DataTypes.STRING, "tieZ"));
    }

    @Test
    public void testWithEmptyTextField() throws Exception {
        assertNormalize("translate('', 'Ct', 'Dk')", isLiteral(""));
    }

    @Test
    public void testWithEmptyFromField() throws Exception {
        // 'C' replaced with 'D', 't' replaced with 'k'
        assertNormalize("translate('Crate', '', 'Dk')", isLiteral("Crate"));
    }

    @Test
    public void testWithEmptyToField() throws Exception {
        assertNormalize("translate('Crate', 're', '')", isLiteral("Cat"));
    }

    @Test
    public void testWithFromFieldLargerThanToField() throws Exception {
        // As from.length() > to.length(), unmatched chars in 'from' are removed
        assertNormalize("translate('Crate', 'rCe', 'c')", isLiteral("cat"));
    }

    @Test
    public void testWithToFieldLargerThanFromField() throws Exception {
        // As to.length() > from.length(), unmatched chars in 'to' are ignored
        assertNormalize("translate('Crate', 'C', 'Dk')", isLiteral("Drate"));
    }

    @Test
    public void testWithDuplicateCharsInFromField() throws Exception {
        assertNormalize("translate('Crate', 'CtC', 'Dk')", isLiteral("Drake"));
    }

    @Test
    public void testWithNullArgument() {
        assertEvaluate("translate(null, 'Ct', 'Dk')", null);
        assertEvaluate("translate('Crate', null, 'Dk')", null);
        assertEvaluate("translate('Crate', 'Ct', null)", null);
    }
}
