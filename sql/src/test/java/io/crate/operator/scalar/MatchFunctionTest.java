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

import com.google.common.collect.ImmutableList;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class MatchFunctionTest {

    private static FunctionInfo functionInfo = new FunctionInfo(
            new FunctionIdent(
                    MatchFunction.NAME,
                    ImmutableList.of(DataType.STRING, DataType.STRING)),
            DataType.BOOLEAN
    );
    private static MatchFunction op = new MatchFunction(functionInfo);

    @Test
    public void testNormalizeSymbol() throws Exception {
        Function matchFunction = new Function(
                op.info(),
                Arrays.<Symbol>asList(new Reference(), new StringLiteral("foo")));
        Symbol result = op.normalizeSymbol(matchFunction);

        assertThat(result, instanceOf(Function.class));
        assertEquals(matchFunction, result);
    }

}
