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

package io.crate.operation.scalar;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.*;
import io.crate.operation.Input;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.Nullable;

import java.util.*;

public class ArrayDifferenceFunction extends Scalar<Object[], Object> {

    public static final String NAME = "array_difference";
    private FunctionInfo functionInfo;
    private final Optional<Set<Object>> optionalSubtractSet;

    private static FunctionInfo createInfo(List<DataType> types) {
        ArrayType arrayType = (ArrayType) types.get(0);
        if(arrayType.innerType().equals(DataTypes.UNDEFINED)){
            arrayType = (ArrayType) types.get(1);
        }
        return new FunctionInfo(new FunctionIdent(NAME, types), arrayType);
    }

    public static void register(ScalarFunctionModule module) {
        module.register(NAME, new Resolver());
    }

    protected ArrayDifferenceFunction(FunctionInfo functionInfo, @Nullable Set<Object> subtractSet) {
        this.functionInfo = functionInfo;
        optionalSubtractSet = Optional.fromNullable(subtractSet);
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
    public Scalar<Object[], Object> compile(List<Symbol> arguments) {

        Symbol symbol = arguments.get(1);
        if(symbol == null){
            return this;
        }

        if (!symbol.symbolType().isValueSymbol()) {
            // arguments are no values, we can't compile
            return this;
        }

        Input input = (Input) symbol;
        if(input.value() == null){
            return this;
        }

        DataType innerType = ((ArrayType) this.info().returnType()).innerType();
        Object[] array = (Object[]) input.value();
        Set<Object> subtractSet = new HashSet<>();
        if(array.length > 0){
            for(Object element : array){
                subtractSet.add(innerType.value(element));
            }
        }

        return new ArrayDifferenceFunction(this.functionInfo, subtractSet);
    }

    @Override
    public Object[] evaluate(Input[] args) {

        if(args[0] == null || args[0].value() == null){
            return null;
        }

        DataType innerType = ((ArrayType) this.info().returnType()).innerType();
        Set<Object> localSubtractSet;
        if(!optionalSubtractSet.isPresent()){
            localSubtractSet = new HashSet<>();
            for(int i = 1; i < args.length; i++){
                if(args[i] == null || args[i].value() == null){
                    continue;
                }
                Object[] array = (Object[]) args[i].value();
                for(Object element : array){
                    localSubtractSet.add(innerType.value(element));
                }
            }
        }else{
            localSubtractSet = optionalSubtractSet.get();
        }

        Object[] originalArray = (Object[]) args[0].value();
        List<Object> resultList   = new ArrayList<>(originalArray.length);
        for(int i = 0; i < originalArray.length; i++){
            Object element = innerType.value(originalArray[i]);
            if(!localSubtractSet.contains(element)){
                resultList.add(element);
            }
        }

        return resultList.toArray();
    }


    private static class Resolver implements DynamicFunctionResolver {

        @Override
        public FunctionImplementation<Function> getForTypes(List<DataType> dataTypes) throws IllegalArgumentException {
            Preconditions.checkArgument(dataTypes.size() == 2, "array_difference function requires 2 arguments");

            for (int i = 0; i < dataTypes.size(); i++) {
                Preconditions.checkArgument(dataTypes.get(i) instanceof ArrayType, String.format(Locale.ENGLISH,
                        "Argument %d of the array_difference function cannot be converted to array", i + 1));
            }

            DataType innerType0 = ((ArrayType) dataTypes.get(0)).innerType();
            DataType innerType1 = ((ArrayType) dataTypes.get(1)).innerType();

            Preconditions.checkArgument(!innerType0.equals(DataTypes.UNDEFINED) || !innerType1.equals(DataTypes.UNDEFINED),
                    "One of the arguments of the array_difference function can be of undefined inner type, but not both");

            if(!innerType0.equals(DataTypes.UNDEFINED)){
                Preconditions.checkArgument(innerType1.isConvertableTo(innerType0),
                        String.format(Locale.ENGLISH,
                                "Second argument's inner type (%s) of the array_difference function cannot be converted to the first argument's inner type (%s)",
                                innerType1,innerType0));
            }

            return new ArrayDifferenceFunction(createInfo(dataTypes), null);
        }
    }
}
