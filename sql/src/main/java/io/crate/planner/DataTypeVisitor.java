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

package io.crate.planner;

import io.crate.planner.symbol.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.UndefinedType;

import java.util.ArrayList;
import java.util.List;

public class DataTypeVisitor extends SymbolVisitor<Void, DataType> {

    private static final DataTypeVisitor INSTANCE = new DataTypeVisitor();
    private DataTypeVisitor() {}

    public static DataType fromSymbol(Symbol symbol) {
        return INSTANCE.process(symbol, null);
    }

    public static List<DataType> fromSymbols(List<? extends Symbol> keys) {
        List<DataType> types = new ArrayList<>(keys.size());
        for (Symbol key : keys) {
            types.add(fromSymbol(key));
        }
        return types;
    }

    @Override
    public DataType visitAggregation(Aggregation symbol, Void context) {
        if (symbol.toStep() == Aggregation.Step.PARTIAL) {
            return UndefinedType.INSTANCE; // TODO: change once we have aggregationState types
        }
        return symbol.functionInfo().returnType();
    }

    @Override
    public DataType visitInputColumn(InputColumn inputColumn, Void context) {
        return inputColumn.valueType();
    }

    @Override
    public DataType visitReference(Reference symbol, Void context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitFunction(Function symbol, Void context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitDynamicReference(DynamicReference symbol, Void context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitParameter(Parameter symbol, Void context) {
        return DataTypes.guessType(symbol.value(), false);
    }

    @Override
    public DataType visitLiteral(Literal symbol, Void context) {
        return symbol.valueType();
    }

    @Override
    public DataType visitValue(Value symbol, Void context) {
        return symbol.valueType();
    }

    @Override
    protected DataType visitSymbol(Symbol symbol, Void context) {
        throw new UnsupportedOperationException(SymbolFormatter.format("Unable to get DataType from symbol: %s", symbol));
    }

}
