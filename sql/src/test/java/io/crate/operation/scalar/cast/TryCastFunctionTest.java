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

package io.crate.operation.scalar.cast;

import io.crate.metadata.*;
import io.crate.operation.scalar.AbstractScalarFunctionsTest;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.junit.Test;

import java.util.Arrays;

import static org.hamcrest.CoreMatchers.instanceOf;

public class TryCastFunctionTest extends AbstractScalarFunctionsTest {

    @Test
    public void testNormalizeSymbol() throws Exception {
        Reference name_ref = new Reference(new ReferenceInfo(
                new ReferenceIdent(new TableIdent(null, "d"), "1"),
                RowGranularity.DOC, DataTypes.STRING)
        );

        Function cast = new Function(
                CastFunctionResolver.functionInfo(DataTypes.STRING, DataTypes.INTEGER),
                Arrays.<Symbol>asList(name_ref)
        );

        FunctionImplementation tryCast = functions.get(new FunctionIdent(TryCastFunction.NAME,
                Arrays.<DataType>asList(DataTypes.INTEGER)));

        Symbol normalized =  tryCast.normalizeSymbol(cast);
        assertThat(normalized, instanceOf(Function.class));
    }
}
