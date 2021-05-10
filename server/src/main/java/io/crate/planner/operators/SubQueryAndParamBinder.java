/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.planner.operators;

import io.crate.data.Row;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.FunctionCopyVisitor;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.ParameterSymbol;
import io.crate.expression.symbol.SelectSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Locale;

public class SubQueryAndParamBinder extends FunctionCopyVisitor<Void>
    implements java.util.function.Function<Symbol, Symbol> {

    private final Row params;
    private final SubQueryResults subQueryResults;

    /**
     * Returns a bound symbol with ParameterSymbols or SelectSymbols replaced as literals using the provided arguments.
     *
     * If multiple calls with the same params and subQueryResults are made it's better to instantiate the class
     * once using {@link #SubQueryAndParamBinder(Row, SubQueryResults)}
     */
    public static Symbol convert(Symbol symbol, Row params, SubQueryResults subQueryResults) {
        SubQueryAndParamBinder binder = new SubQueryAndParamBinder(params, subQueryResults);
        return binder.apply(symbol);
    }

    public SubQueryAndParamBinder(Row params, SubQueryResults subQueryResults) {
        this.params = params;
        this.subQueryResults = subQueryResults;
    }

    @Override
    public Symbol visitParameterSymbol(ParameterSymbol parameterSymbol, Void context) {
        return convert(parameterSymbol, params);
    }

    @Override
    public Symbol visitSelectSymbol(SelectSymbol selectSymbol, Void context) {
        Object value = subQueryResults.getSafe(selectSymbol);
        return Literal.ofUnchecked(selectSymbol.valueType(), selectSymbol.valueType().sanitizeValue(value));
    }

    @Override
    public Symbol apply(Symbol symbol) {
        return symbol.accept(this, null);
    }

    private static Symbol convert(ParameterSymbol parameterSymbol, Row params) {
        DataType<?> type = parameterSymbol.valueType();
        Object value;
        try {
            value = params.get(parameterSymbol.index());
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "The query contains a parameter placeholder $%d, but there are only %d parameter values",
                (parameterSymbol.index() + 1),
                params.numColumns()
            ));
        }
        if (type.equals(DataTypes.UNDEFINED)) {
            type = DataTypes.guessType(value);
        }
        try {
            return Literal.ofUnchecked(type, type.implicitCast(value));
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ConversionException(value, type);
        }
    }
}
