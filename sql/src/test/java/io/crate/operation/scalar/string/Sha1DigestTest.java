
/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.scalar.string;

import static io.crate.testing.TestingHelpers.isLiteral;

import org.junit.Test;

import io.crate.analyze.symbol.Literal;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.types.DataTypes;

public class Sha1DigestTest extends AbstractScalarFunctionsTest{
	
	@Test
    public void testSHA1() throws Exception {
        assertNormalize("sha1('abc')", isLiteral("a9993e364706816aba3e25717850c26c9cd0d89d"));
        assertNormalize("sha1('crate is the best')", isLiteral("65c539a23aa541a7e217b78d9b94a9ac0f58985d"));
        assertNormalize("sha1('last test with umlaut ä ö ü Ö=')", isLiteral("8f870b51c736421aa92dc3febd2df61a345b80f1"));
    }

    @Test
    public void testEvaluateNull() throws Exception {
        assertEvaluate("sha1(name)", null, Literal.newLiteral(DataTypes.STRING, null));
    }
}
