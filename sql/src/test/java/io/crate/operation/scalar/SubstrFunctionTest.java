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

package io.crate.operation.scalar;

import io.crate.analyze.symbol.Literal;
import io.crate.testing.TestingHelpers;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.junit.Test;

import static io.crate.testing.TestingHelpers.isLiteral;
import static org.hamcrest.core.Is.is;

public class SubstrFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeSymbol() throws Exception {
        assertNormalize("substr('cratedata', 0)", isLiteral("cratedata"));
        assertNormalize("substr('cratedata', 6)", isLiteral("data"));
        assertNormalize("substr('cratedata', 10)", isLiteral(""));
        assertNormalize("substr('cratedata', 1, 1)", isLiteral("c"));
        assertNormalize("substr('cratedata', 3, 2)", isLiteral("at"));
        assertNormalize("substr('cratedata', 6, 10)", isLiteral("data"));
        assertNormalize("substr('cratedata', 6, 0)", isLiteral(""));
        assertNormalize("substr('cratedata', 10, -1)", isLiteral(""));
    }

    @Test
    public void testSubstring() throws Exception {
        assertThat(SubstrFunction.substring(new BytesRef("cratedata"), 2, 5), is(new BytesRef("ate")));
        assertThat(SubstrFunction.substring(TestingHelpers.addOffset(new BytesRef("cratedata")), 2, 5),
                                            is(new BytesRef("ate")));
    }

    @Test
    public void testEvaluate() throws Exception {
        assertEvaluate("substr('cratedata', 6, 2)", "da");
    }

    @Test
    public void testEvaluateWithArgsAsNonLiterals() throws Exception {
        assertEvaluate("substr('cratedata', id, id)", "crate", Literal.newLiteral(1L), Literal.newLiteral(5L));
    }

    @Test
    public void testEvaluateWithArgsAsNonLiteralsIntShort() throws Exception {
        assertEvaluate("substr(name, id, id)", "crate",
            Literal.newLiteral("cratedata"),
            Literal.newLiteral(DataTypes.SHORT, (short) 1),
            Literal.newLiteral(DataTypes.SHORT, (short) 5));
    }

    @Test
    public void testNullInputs() throws Exception {
        assertEvaluate("substr(name, id, id)", null,
            Literal.newLiteral(DataTypes.STRING, null),
            Literal.newLiteral(1),
            Literal.newLiteral(1));
        assertEvaluate("substr(name, id, id)", null,
            Literal.newLiteral("crate"),
            Literal.newLiteral(DataTypes.INTEGER, null),
            Literal.newLiteral(1));
        assertEvaluate("substr(name, id, id)", null,
            Literal.newLiteral("crate"),
            Literal.newLiteral(1),
            Literal.newLiteral(DataTypes.SHORT, null));
    }
}

