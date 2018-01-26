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

package io.crate.planner.operators;

import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.data.Row;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Map;

public class SubQueryAndParamBinder extends FunctionCopyVisitor<Void>
    implements java.util.function.Function<Symbol, Symbol> {

    private final Row params;
    private final Map<SelectSymbol, Object> subQueryValues;

    public static Symbol convert(Symbol symbol, Row params, Map<SelectSymbol, Object> subQueryValues) {
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryValues);
        return binder.apply(symbol);
    }

    public SubQueryAndParamBinder(Row params, Map<SelectSymbol, Object> subQueryValues) {
        this.params = params;
        this.subQueryValues = subQueryValues;
    }

    @Override
    public Symbol visitParameterSymbol(ParameterSymbol parameterSymbol, Void context) {
        return convert(parameterSymbol, params);
    }

    @Override
    public Symbol visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
        Object value = subQueryValues.get(selectSymbol);
        if (value == null && !subQueryValues.containsKey(selectSymbol)) {
            throw new IllegalArgumentException("Couldn't resolve subQuery: " + selectSymbol);
        }
        return Literal.of(selectSymbol.valueType(), selectSymbol.valueType().value(value));
    }

    @Override
    public Symbol apply(Symbol symbol) {
        return process(symbol, null);
    }

    private static Symbol convert(ParameterSymbol parameterSymbol, Row params) {
        DataType type = parameterSymbol.valueType();
        Object value = params.get(parameterSymbol.index());
        if (type.equals(DataTypes.UNDEFINED)) {
            type = DataTypes.guessType(value);
        }
        return Literal.of(type, type.value(value));
    }
}
