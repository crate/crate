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

package io.crate.expression.predicate;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class IsNullPredicateTest extends ScalarTestCase {

    @Test
    public void testNormalizeSymbolFalse() throws Exception {
        assertNormalize("'a' is null", isLiteral(false));
    }

    @Test
    public void testNormalizeSymbolTrue() throws Exception {
        assertNormalize("null is null", isLiteral(true));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("name is null", isFunction(IsNullPredicate.NAME));
    }

    @Test
    public void testNormalizeUndefinedType() {
        assertNormalize("obj_ignored['column'] is null", isFunction(IsNullPredicate.NAME));
    }

    @Test
    public void testEvaluateWithInputThatReturnsNull() throws Exception {
        assertEvaluate("name is null", true, Literal.of(DataTypes.STRING, null));
    }
}

