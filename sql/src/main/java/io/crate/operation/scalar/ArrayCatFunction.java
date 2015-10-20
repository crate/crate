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

import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Locale;

public class ArrayCatFunction extends Scalar<Object[], Object> {

    public static final String NAME = "array_cat";
    private FunctionInfo functionInfo;

    public static FunctionInfo createInfo(List<DataType> types) {
        ArrayType arrayType = (ArrayType) types.get(0);
        if(arrayType.innerType().equals(DataTypes.UNDEFINED)){
            arrayType = (ArrayType) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    protected ArrayCatFunction(FunctionInfo functionInfo) {
        this.functionInfo = functionInfo;
    }

    @Override
    public FunctionInfo info() {
        return functionInfo;
    }

    @Override
    public Symbol normalizeSymbol(Function symbol) {
        if (anyNonLiterals(symbol.arguments())) {
            return symbol;
        }


        Input[] argList = new Input[symbol.arguments().size()];
        int counter = 0;
        for(Symbol argSymbol : symbol.arguments()){
            Input input  = (Input) argSymbol;
            argList[counter++] = input;
        }

        return Literal.newLiteral(evaluate(argList), this.info().returnType());
    }

    @Override
    public Object[] evaluate(Input[] args) {
        int counter = 0;
        for(Input array : args){
            if(array == null || array.value() == null){
                continue;
            }
            Object[] arg = (Object[]) array.value();
            counter += arg.length;
        }

        DataType innerType = ((ArrayType) this.info().returnType()).innerType();

        Object[] resultArray = new Object[counter];
        counter = 0;
        for(Input array : args){
            if(array == null || array.value() == null){
                continue;
            }
            Object[] arg = (Object[]) array.value();
            for(Object element: arg){
                resultArray[counter++] = innerType.value(element);
            }
        }

        return resultArray;
    }


    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 2, "array_cat function requires 2 arguments");

            for (int i = 0; i < dataTypes.size(); i++) {
                Preconditions.checkArgument(dataTypes.get(i) instanceof ArrayType, String.format(Locale.ENGLISH,
                        "Argument %d of the array_cat function cannot be converted to array", i + 1));
            }

            DataType innerType0 = ((ArrayType) dataTypes.get(0)).innerType();
            DataType innerType1 = ((ArrayType) dataTypes.get(1)).innerType();

            Preconditions.checkArgument(!innerType0.equals(DataTypes.UNDEFINED) || !innerType1.equals(DataTypes.UNDEFINED),
                    "One of the arguments of the array_cat function can be of undefined inner type, but not both");

            if(!innerType0.equals(DataTypes.UNDEFINED)){
                Preconditions.checkArgument(innerType1.isConvertableTo(innerType0),
                        String.format(Locale.ENGLISH,
                                "Second argument's inner type (%s) of the array_cat function cannot be converted to the first argument's inner type (%s)",
                                innerType1,innerType0));
            }

            return new ArrayCatFunction(createInfo(dataTypes));
        }
    }
}
