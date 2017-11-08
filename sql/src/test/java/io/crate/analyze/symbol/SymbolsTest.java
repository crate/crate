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

package io.crate.analyze.symbol;

import com.google.common.collect.ImmutableMap;
import io.crate.test.integration.CrateUnitTest;
import io.crate.testing.SqlExpressions;
import io.crate.testing.T3;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SymbolsTest extends CrateUnitTest {

    private SqlExpressions expressions = new SqlExpressions(ImmutableMap.of(T3.T1, T3.TR_1), T3.TR_1);

    public void testTryUpcast() {
        Symbol symbol = expressions.asSymbol("x");
        assertThat(Symbols.tryUpcast(symbol, expressions.asSymbol("i")), is(symbol));
        assertThat(Symbols.tryUpcast(expressions.asSymbol("x"), expressions.asSymbol("i::long")),
            is(expressions.asSymbol("x::long")));
        assertThat(Symbols.tryUpcast(expressions.asSymbol("x::long"), expressions.asSymbol("i")), nullValue());
        assertThat(Symbols.tryUpcast(expressions.asSymbol("a"), expressions.asSymbol("[1,2,3]")), nullValue());
    }
}
