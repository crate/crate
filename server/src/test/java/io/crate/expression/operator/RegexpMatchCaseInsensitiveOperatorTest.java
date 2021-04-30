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
import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Test;

import static org.hamcrest.Matchers.is;

public class RegexpMatchCaseInsensitiveOperatorTest extends ScalarTestCase {

    @Test
    public void testNormalize() throws Exception {
        assertNormalize("'' ~* ''", is(Literal.of(true)));
        assertNormalize("'abc' ~* 'a.c'", is(Literal.of(true)));
        assertNormalize("'AbC' ~* 'a.c'", is(Literal.of(true)));
        assertNormalize("'abbbbc' ~* 'a(b{1,4})c'", is(Literal.of(true)));
        assertNormalize("'abc' ~* 'a~bc'", is(Literal.of(false)));           // no PCRE syntax, should fail
        assertNormalize("'100 €' ~* '<10-101> €|$'", is(Literal.of(false))); // no PCRE syntax, should fail
    }

    @Test
    public void testNormalizeNull() throws Exception {
        assertNormalize("null ~* 'foo'", is(Literal.of(DataTypes.BOOLEAN, null)));
        assertNormalize("'foo' ~* null", is(Literal.of(DataTypes.BOOLEAN, null)));
        assertNormalize("null ~* null", is(Literal.of(DataTypes.BOOLEAN, null)));
    }

    // evaluate
    @Test
    public void testEvaluate() throws Exception {
        // case-insensitive matching should work
        assertEvaluate("'foo bar' ~* '([A-Z][^ ]+ ?){2}'", true);
        assertEvaluate("'Foo Bar' ~* '([A-Z][^ ]+ ?){2}'", true);
        assertEvaluate("'' ~* ''", true);
        // java.util.regex does not understand proprietary syntax of `dk.brics.automaton` (no PCRE, should fail)
        assertEvaluate("'1000 $' ~* '(<1-9999>) $|€'", false);
        assertEvaluate("'10000 $' ~* '(<1-9999>) $|€'", false);
    }
}
