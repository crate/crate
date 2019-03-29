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

package io.crate.expression.scalar.postgres;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;

public class CurrentSettingFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalize() {
        assertNormalize("current_setting('search_path')", isLiteral("pg_catalog, doc"));
        assertNormalize("current_setting('enable_semijoin')", isLiteral("false"));
        assertNormalize("current_setting('enable_hashjoin')", isLiteral("true"));
    }

    @Test
    public void testNormalizeWithPgCatalogPrefix() {
        assertNormalize("pg_catalog.current_setting('search_path')", isLiteral("pg_catalog, doc"));
    }

    @Test
    public void testEvaluate() {
        assertEvaluate("current_setting(name)", "pg_catalog, doc", Literal.of("search_path"));
    }

    @Test
    public void testEvaluateUnrecognisedValue() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unrecognised Setting");
        assertEvaluate("current_setting(name)", "", Literal.of("foo"));
    }

}
