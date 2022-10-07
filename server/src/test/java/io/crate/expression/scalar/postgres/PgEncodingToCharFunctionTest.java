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

import static io.crate.testing.Asserts.isLiteral;

import org.junit.Test;

import io.crate.expression.scalar.ScalarTestCase;

public class PgEncodingToCharFunctionTest extends ScalarTestCase {
    @Test
    public void testPgEncodingToChar() throws Exception {
        assertEvaluateNull("pg_encoding_to_char(-1)");
        assertEvaluateNull("pg_encoding_to_char(null)");
        assertEvaluate("pg_encoding_to_char(1)", "EUC_JP");
        assertEvaluate("pg_encoding_to_char(6)", "UTF8");
        assertEvaluate("pg_encoding_to_char(41)", "SHIFT_JIS_2004");
        assertEvaluateNull("pg_encoding_to_char(42)");
        assertNormalize("pg_encoding_to_char(6)", isLiteral("UTF8"));
    }

    @Test
    public void testPgEncodingToCharWithFQNFunctionName() throws Exception {
        assertEvaluate("pg_catalog.pg_encoding_to_char(6)", "UTF8");
    }
}
