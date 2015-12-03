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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.operation.Input;
import io.crate.types.ArrayType;
import io.crate.types.DataType;

import java.util.Arrays;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ArrayCastTest {

    @SuppressWarnings("unchecked")
    public static Object[] evalArrayCast(Functions functions, String functionName, Object objects, DataType innerType) {
        final DataType arrayType = new ArrayType(innerType);
        Scalar impl = (Scalar) functions.get(new FunctionIdent(functionName, ImmutableList.of(arrayType)));

        Literal input = Literal.newLiteral(arrayType, objects);
        Symbol normalized = impl.normalizeSymbol(new Function(impl.info(), Arrays.<Symbol>asList(input)));
        Object[] integers = (Object[]) impl.evaluate(input);

        assertThat(integers, is(((Input) normalized).value()));
        return integers;

    }
}
