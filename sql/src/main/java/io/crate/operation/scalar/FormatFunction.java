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

package io.crate.operation.scalar;

import com.google.common.base.Preconditions;
import io.crate.DataType;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.planner.symbol.*;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

public class FormatFunction implements Scalar<BytesRef, Object>, DynamicFunctionResolver {

    public static final String NAME = "format";
    private FunctionInfo info;

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new FormatFunction());
    }

    private static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), DataType.STRING);
    }

    FormatFunction() {}

    FormatFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public BytesRef evaluate(Input<Object>... args) {
        assert args.length > 1;
        assert args[0].value() != null;

        Object[] values = new Object[args.length - 1];
        for (int i = 0; i < args.length - 1; i++) {
            values[i] = args[i + 1].value();
        }

        Object formatString = args[0].value();
        assert formatString instanceof BytesRef;
        return new BytesRef(String.format(((BytesRef)formatString).utf8ToString(), values));
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function function) {
        assert (function.arguments().size() > 1);

        Symbol formatString = function.arguments().get(0);
        if (formatString.symbolType() != SymbolType.STRING_LITERAL) {
            // probably something like   format(column_with_format_string, arg1) ?
            return function;
        }

        assert formatString instanceof StringLiteral;
        List<Object> objects = new ArrayList<>();
        List<Symbol> arguments = function.arguments().subList(1, function.arguments().size());

        for (Symbol argument : arguments) {
            if (!argument.symbolType().isLiteral()) {
                return function; // can't normalize if arguments still contain non-literals
            }

            if (argument.symbolType() == SymbolType.STRING_LITERAL) {
                objects.add(((StringLiteral)argument).valueAsString());
            } else {
                objects.add(((Literal) argument).value());
            }
        }

        return new StringLiteral(String.format(
                ((StringLiteral) formatString).valueAsString(),
                objects.toArray(new Object[objects.size()])));
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() > 1 && dataTypes.get(0) == DataType.STRING);
        return new FormatFunction(createInfo(dataTypes));
    }
}
