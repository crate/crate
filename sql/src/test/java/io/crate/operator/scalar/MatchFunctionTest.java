/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operator.scalar;

import io.crate.planner.symbol.BooleanLiteral;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.StringLiteral;
import io.crate.planner.symbol.Symbol;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class MatchFunctionTest {

    @Test
    public void testNormalizeSymbolNoMatch() throws Exception {
        MatchFunction function = new MatchFunction();

        Function matchFunction = new Function(
                MatchFunction.INFO,
                // behaves like eqOperator on literals, so this is no match since no analyzer is in use
                Arrays.<Symbol>asList(new StringLiteral("foo bar"), new StringLiteral("foo")));
        Symbol normalized = function.normalizeSymbol(matchFunction);

        assertThat(normalized, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral)normalized).value(), is(false));
    }

    @Test
    public void testNormalizeSymbolMatch() throws Exception {
        MatchFunction function = new MatchFunction();

        Function matchFunction = new Function(
                MatchFunction.INFO,
                Arrays.<Symbol>asList(new StringLiteral("foo"), new StringLiteral("foo")));
        Symbol normalized = function.normalizeSymbol(matchFunction);

        assertThat(normalized, instanceOf(BooleanLiteral.class));
        assertThat(((BooleanLiteral)normalized).value(), is(true));
    }
}
