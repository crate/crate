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

import io.crate.expression.scalar.ScalarTestCase;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class BitwiseAndOperatorTest extends ScalarTestCase {

    @Test
    public void testBitwiseAnd() {
        assertNormalize("id & 8", isFunction("op_&"));
        assertNormalize("4 & 20", isLiteral(true));
        assertNormalize("16 & 20", isLiteral(true));
        assertNormalize("1 & 20", isLiteral(false));
        assertNormalize("2 & 20", isLiteral(false));
        assertNormalize("1 & 3", isLiteral(true));
        assertNormalize("2 & 3", isLiteral(true));
        assertNormalize("4 & 3", isLiteral(false));
        assertNormalize("16 & 3", isLiteral(false));
        assertEvaluate("null & null", null);
        assertEvaluate("20 & null", null);
        assertEvaluate("null & 3", null);
    }
}
