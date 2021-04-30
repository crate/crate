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

package io.crate.expression.operator;

import io.crate.expression.symbol.Literal;
import io.crate.expression.scalar.ScalarTestCase;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class CmpOperatorTest extends ScalarTestCase {

    @Test
    public void testLte() {
        assertNormalize("id <= 8", isFunction("op_<="));
        assertNormalize("8 <= 200", isLiteral(true));
        assertNormalize("0.1 <= 0.1", isLiteral(true));
        assertNormalize("16 <= 8", isLiteral(false));
        assertNormalize("'abc' <= 'abd'", isLiteral(true));
        assertEvaluate("true <= null", null);
        assertEvaluate("null <= 1", null);
        assertEvaluate("null <= 'abc'", null);
        assertEvaluate("null <= null", null);
    }

    @Test
    public void testLt() {
        assertNormalize("id < 8", isFunction("op_<"));
        assertNormalize("0.1 < 0.2", isLiteral(true));
        assertNormalize("'abc' < 'abd'", isLiteral(true));
        assertEvaluate("true < null", null);
        assertEvaluate("null < 1", null);
        assertEvaluate("null < name", null, Literal.of("foo"));
        assertEvaluate("null < null", null);
    }

    @Test
    public void testGte() {
        assertNormalize("id >= 8", isFunction("op_>="));
        assertNormalize("0.1 >= 0.1", isLiteral(true));
        assertNormalize("16 >= 8", isLiteral(true));
        assertNormalize("'abc' >= 'abd'", isLiteral(false));
        assertEvaluate("true >= null", null);
        assertEvaluate("null >= 1", null);
        assertEvaluate("null >= 'abc'", null);
        assertEvaluate("null >= null", null);
    }

    @Test
    public void testGt() {
        assertNormalize("id > 200", isFunction("op_>"));
        assertNormalize("0.1 > 0.1", isLiteral(false));
        assertNormalize("16 > 8", isLiteral(true));
        assertNormalize("'abd' > 'abc'", isLiteral(true));
        assertEvaluate("true > null", null);
        assertEvaluate("null > 1", null);
        assertEvaluate("name > null", null, Literal.of("foo"));
        assertEvaluate("null > null", null);
    }

    @Test
    public void testBetween() {
        assertNormalize("0.1 between 0.01 and 0.2", isLiteral(true));
        assertNormalize("10 between 1 and 2", isLiteral(false));
        assertNormalize("'abd' between 'abc' and 'abe'", isLiteral(true));
        assertEvaluate("1 between 0 and null", null);
        assertEvaluate("1 between null and 10", null);
        assertEvaluate("1 between null and null", null);
        assertEvaluate("null between 1 and 10", null);
        assertEvaluate("null between 1 and null", null);
        assertEvaluate("null between null and 10", null);
        assertEvaluate("null between null and null", null);
    }
}
