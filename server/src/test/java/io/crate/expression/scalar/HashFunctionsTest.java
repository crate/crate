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

package io.crate.expression.scalar;

import io.crate.expression.symbol.Literal;
import io.crate.types.DataTypes;
import org.junit.Test;

import static io.crate.testing.SymbolMatchers.isFunction;
import static io.crate.testing.SymbolMatchers.isLiteral;

public class HashFunctionsTest extends ScalarTestCase {

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("md5('©rate')", "53cee571b9fbab07cc894d55988cc70b");
        assertEvaluate("md5('crate')", "dd4827af87b26de9ed92e6fb08efc5ab");
        assertEvaluate("md5('')", "d41d8cd98f00b204e9800998ecf8427e");
        assertEvaluate("sha1('©rate')", "9a437faeb9adff59cc06313bfb23fe1d46181924");
        assertEvaluate("sha1('crate')", "1673dc397042322a0a5ac49c79cc08d3a25cb0f6");
        assertEvaluate("sha1('')", "da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

    @Test
    public void testNormalize() {
        assertNormalize("md5('©rate')", isLiteral("53cee571b9fbab07cc894d55988cc70b"));
        assertNormalize("md5('crate')", isLiteral("dd4827af87b26de9ed92e6fb08efc5ab"));
        assertNormalize("md5('')", isLiteral("d41d8cd98f00b204e9800998ecf8427e"));
        assertNormalize("sha1('©rate')", isLiteral("9a437faeb9adff59cc06313bfb23fe1d46181924"));
        assertNormalize("sha1('crate')", isLiteral("1673dc397042322a0a5ac49c79cc08d3a25cb0f6"));
        assertNormalize("sha1('')", isLiteral("da39a3ee5e6b4b0d3255bfef95601890afd80709"));
    }

    @Test
    public void evaluateOnNull() throws Exception {
        assertEvaluate("md5(null)", null);
        assertEvaluate("md5(null)", null, Literal.of(DataTypes.STRING, null));
        assertEvaluate("sha1(null)", null);
        assertEvaluate("sha1(name)", null, Literal.of(DataTypes.STRING, null));
    }

    @Test
    public void testNormalizeReference() throws Exception {
        assertNormalize("md5(name)", isFunction("md5"));
        assertEvaluate("md5(name)", "dd4827af87b26de9ed92e6fb08efc5ab", Literal.of("crate"));
        assertNormalize("sha1(name)", isFunction("sha1"));
        assertEvaluate("sha1(name)", "1673dc397042322a0a5ac49c79cc08d3a25cb0f6", Literal.of("crate"));
    }

    /*
     * Integration test to ensure checksums can be used with String operations.
     */
    @Test
    public void testConcatenation() throws Exception {
        assertEvaluate("'crate ' || sha1('')", "crate da39a3ee5e6b4b0d3255bfef95601890afd80709");
    }

}
