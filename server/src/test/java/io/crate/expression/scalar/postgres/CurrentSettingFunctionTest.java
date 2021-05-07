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

package io.crate.expression.scalar.postgres;

import io.crate.expression.scalar.ScalarTestCase;
import io.crate.expression.symbol.Literal;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class CurrentSettingFunctionTest extends ScalarTestCase {


    @Test
    public void testNormalizeExistingSettingSingleArg() {
        assertNormalize("current_setting('search_path')", isLiteral("pg_catalog, doc"));
        assertNormalize("current_setting('enable_hashjoin')", isLiteral("true"));
    }

    @Test
    public void testNormalizeNonExistingSettingSingleArg() {
        // eval is expected to throw an exception thus the function symbol is return
        assertNormalize("current_setting('foo')",
                        isFunction("current_setting", isLiteral("foo"))
        );
    }

    @Test
    public void testNormalizeExistingSettingWithMissingOKArgument() {
        assertNormalize("current_setting('search_path', true)", isLiteral("pg_catalog, doc"));
        assertNormalize("current_setting('search_path', false)", isLiteral("pg_catalog, doc"));
    }

    @Test
    public void testNormalizeNonExistingSettingWithMissingOKArgumentAsTrue() {
        assertNormalize("current_setting('foo', true)", isLiteral(null));
    }

    @Test
    public void testNormalizeNonExistingSettingWithMissingOKArgumentAsFalse() {
        // eval is expected to throw an exception thus the function symbol is return
        assertNormalize("current_setting('foo', false)",
                        isFunction("current_setting",
                                   isLiteral("foo"),
                                   isLiteral(false))
        );
    }

    @Test
    public void testNormalizeWithPgCatalogPrefix() {
        assertNormalize("pg_catalog.current_setting('enable_hashjoin')", isLiteral("true"));
    }

    @Test
    public void testNormalizeWithNulls() {
        assertNormalize("current_setting(null)", isLiteral(null));
        assertNormalize("current_setting('search_path', null)", isLiteral(null));
        assertNormalize("current_setting(null, null)", isLiteral(null));
    }

    @Test
    public void testEvaluateExistingSettingSingleArgument() {
        assertEvaluate("current_setting(name)", "pg_catalog, doc", Literal.of("search_path"));
    }

    @Test
    public void testEvaluateExistingSettingWithMissingOKArgument() {
        assertEvaluate("current_setting(name, true)", "pg_catalog, doc", Literal.of("search_path"));
        assertEvaluate("current_setting(name, false)", "pg_catalog, doc", Literal.of("search_path"));
    }

    @Test
    public void testEvaluateNonExistingSettingSingleArgument() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unrecognised Setting");
        assertEvaluate("current_setting(name)", "", Literal.of("foo"));
    }

    @Test
    public void testEvaluateNonExistingSettingWithMissingOKArgumentAsTrue() {
        assertEvaluate("current_setting(name, true)", null, Literal.of("foo"));
    }

    @Test
    public void testEvaluateNonExistingSettingWithMissingOKArgumentAsFalse() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unrecognised Setting");
        assertEvaluate("current_setting(name, false)", "", Literal.of("foo"));
    }

    @Test
    public void testEvaluateWithNulls() {
        assertEvaluate("current_setting(name)", null, Literal.NULL);
        assertEvaluate("current_setting(name, is_awesome)", null, Literal.of("search_path"), Literal.NULL);
        assertEvaluate("current_setting(name, is_awesome)", null, Literal.NULL, Literal.NULL);
    }
}
