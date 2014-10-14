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

package io.crate.operation.scalar.cast;

import com.google.common.base.Preconditions;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.operation.scalar.ScalarFunctionModule;
import io.crate.planner.symbol.Function;
import io.crate.planner.symbol.Literal;
import io.crate.planner.symbol.Symbol;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.List;
import java.util.Locale;

public class ToStringArrayFunction extends Scalar<BytesRef[], Object> implements DynamicFunctionResolver {

    public static final String NAME = "toStringArray";
    private static final DataType arrayStringType = new ArrayType(DataTypes.STRING);

    private static FunctionInfo createInfo(List<DataType> types) {
        return new FunctionInfo(new FunctionIdent(NAME, types), new ArrayType(types.get(0)));
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new ToStringArrayFunction());
    }

    private FunctionInfo info;

    private ToStringArrayFunction() {
    }

    public ToStringArrayFunction(FunctionInfo info) {
        this.info = info;
    }

    @Override
    public FunctionInfo info() {
        return info;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        final int size = symbol.arguments().size();
        assert size == 1 : "Invalid number of arguments";

        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }
        final Symbol input = symbol.arguments().get(0);
        final Object inputValue = ((Input) input).value();
        if (inputValue == null) {
            return Literal.NULL;
        }

        return Literal.newLiteral(evaluate(new Input[]{(Input)input}), arrayStringType);
    }

    @Override
    public BytesRef[] evaluate(Input[] args) {
        assert args.length == 1 : "Invalid number of arguments";
        Object[] collection = (Object[]) args[0].value();
        if (collection instanceof BytesRef[]) {
            return (BytesRef[]) collection;
        }
        BytesRef[] output = new BytesRef[collection.length];
        for (int i = 0; i < collection.length; i++) {
            if (collection[i] instanceof BytesRef) {
                output[i] = (BytesRef) collection[i];
                continue;
            }
            output[i] = DataTypes.STRING.value(collection[i]);
        }
        return output;
    }

    @Override
    public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
        Preconditions.checkArgument(dataTypes.size() == 1, "Invalid number of arguments");
        Preconditions.checkArgument(DataTypes.isCollectionType(dataTypes.get(0)), "Argument must be a collection type");
        ArrayType arrayType = (ArrayType) dataTypes.get(0);
        // TODO: support geo inner types
        Preconditions.checkArgument(DataTypes.PRIMITIVE_TYPES.contains(arrayType.innerType()),
                String.format(Locale.ENGLISH, "Array inner type '%s' not supported for conversion",
                        arrayType.innerType().getName()));
        return new ToStringArrayFunction(createInfo(dataTypes));
    }

}
