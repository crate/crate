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

package io.crate.expression.operator.any;

import io.crate.expression.scalar.ScalarTestCase;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class AnyEqOperatorTest extends ScalarTestCase {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("1 = ANY([1])", true);
        assertEvaluate("1 = ANY([2])", false);

        assertEvaluate("{i=1, b=true} = ANY([{i=1, b=true}])", true);
        assertEvaluate("{i=1, b=true} = ANY([{i=2, b=true}])", false);
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluate("null = ANY(null)", null);
        assertEvaluate("42 = ANY(null)", null);
        assertEvaluate("null = ANY([1])", null);
    }

    @Test
    public void testNormalizeSymbolNull() throws Exception {
        assertNormalize("null = ANY([null])", isLiteral(null));
        assertNormalize("42 = ANY([null])", isLiteral(null));
        assertNormalize("null = ANY([1])", isLiteral(null));
    }

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("42 = ANY([42])", isLiteral(true));
        assertNormalize("42 = ANY([1, 42, 2])", isLiteral(true));
        assertNormalize("42 = ANY([42])", isLiteral(true));
        assertNormalize("42 = ANY([41, 43, -42])", isLiteral(false));
    }

    @Test
    public void testArrayEqAnyNestedArrayMatches() {
        assertNormalize("['foo', 'bar'] = ANY([ ['foobar'], ['foo', 'bar'], [] ])", isLiteral(true));
    }

    @Test
    public void testArrayEqAnyNestedArrayDoesNotMatch() {
        assertNormalize("['foo', 'bar'] = ANY([ ['foobar'], ['foo', 'ar'], [] ])", isLiteral(false));
    }
}
