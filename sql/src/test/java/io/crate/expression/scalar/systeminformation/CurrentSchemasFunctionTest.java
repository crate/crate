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

package io.crate.expression.scalar.systeminformation;

import io.crate.expression.scalar.AbstractScalarFunctionsTest;
import io.crate.expression.symbol.Literal;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;


public class CurrentSchemasFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeWithDefaultSchemas() {
        assertNormalize("current_schemas(true)", isLiteral(new String[]{"pg_catalog", "doc"}), false);
    }

    @Test
    public void testNormalizeWithFQNFunctionName() {
        assertNormalize("pg_catalog.current_schemas(true)", isLiteral(new String[]{"pg_catalog", "doc"}), false);
    }

    @Test
    public void testNormalizeWithDefaultSchemasNoImplicit() {
        assertNormalize("current_schemas(false)", isLiteral(new String[]{"doc"}), false);
    }

    @Test
    public void testNormalizeWithCustomSchemas() {
        sqlExpressions.setSearchPath("foo", "bar");
        assertNormalize("current_schemas(true)", isLiteral(new String[]{"pg_catalog", "foo", "bar"}), false);
    }

    @Test
    public void testEvaluateCurrentSchema() {
        assertEvaluate("current_schemas(is_awesome)", new String[]{"doc"}, Literal.BOOLEAN_FALSE);
    }

    @Test
    public void testEvaluateNull() {
        assertEvaluate("current_schemas(is_awesome)", new String[]{"doc"}, Literal.of((Boolean) null));
    }
}
