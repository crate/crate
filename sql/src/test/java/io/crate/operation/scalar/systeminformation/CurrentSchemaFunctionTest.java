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

package io.crate.operation.scalar.systeminformation;

import io.crate.analyze.symbol.Function;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.testing.SqlExpressions;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isLiteral;


public class CurrentSchemaFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeCurrentSchemaDefaultSchema() throws Exception {
        sqlExpressions = new SqlExpressions(tableSources);
        functions = sqlExpressions.getInstance(Functions.class);
        assertNormalize("current_schema()", isLiteral("doc"), false);
    }

    @Test
    public void testNormalizeCurrentSchemaCustomSchema() throws Exception {
        sqlExpressions = new SqlExpressions(tableSources);
        sqlExpressions.setDefaultSchema("custom_schema");
        functions = sqlExpressions.getInstance(Functions.class);
        assertNormalize("current_schema()", isLiteral("custom_schema"), false);
    }

    @Test
    public void testEvaluateCurrentSchemaNotSupported() throws Exception {
        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage("Cannot evaluate CURRENT_SCHEMA function.");
        Function function = (Function) sqlExpressions.asSymbol("current_schema()");
        FunctionIdent ident = function.info().ident();
        Scalar impl = (Scalar) functions.getBuiltin(ident.name(), ident.argumentTypes());
        impl.evaluate();
    }
}
