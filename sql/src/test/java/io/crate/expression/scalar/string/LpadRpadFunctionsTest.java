/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.expression.scalar.string;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.hamcrest.core.IsSame;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;
import static org.hamcrest.Matchers.not;

public class LpadRpadFunctionsTest extends AbstractScalarFunctionsTest {

    @Test
    public void testCast() {
        assertNormalize("lpad('hi', '5', 'xy')", isLiteral("xyxhi"));
    }

    @Test
    public void test() {
        assertNormalize("lpad('hi', 5, 'xy')", isLiteral("xyxhi"));
    }

    @Test
    public void testNegativeLen() {
        assertNormalize("lpad('hi', -5, 'xy')", isLiteral(""));
    }

    @Test
    public void testTruncate() {
        assertNormalize("lpad('hello', 2)", isLiteral("he"));
    }

    @Test
    public void testFillSpaceByDefault() {
        assertNormalize("lpad('hi', 5)", isLiteral("   hi"));
    }

    @Test
    public void tests() {
        assertNormalize("lpad('hi', 5, '')", isLiteral("hi"));
    }
}
