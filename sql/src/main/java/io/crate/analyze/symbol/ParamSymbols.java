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

package io.crate.analyze.symbol;

import io.crate.data.Row;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public final class ParamSymbols extends FunctionCopyVisitor<Row> {

    private static final ParamSymbols INSTANCE = new ParamSymbols();

    private ParamSymbols() {
    }

    public static Symbol toLiterals(Symbol st, Row params) {
        return INSTANCE.process(st, params);
    }

    @Override
    protected Symbol visitSymbol(Symbol symbol, Row params) {
        return symbol;
    }

    @Override
    public Symbol visitParameterSymbol(ParameterSymbol parameterSymbol, Row params) {
        return convert(parameterSymbol, params);
    }

    public static Symbol convert(ParameterSymbol parameterSymbol, Row params) {
        DataType type = parameterSymbol.valueType();
        Object value = params.get(parameterSymbol.index());
        if (type.equals(DataTypes.UNDEFINED)) {
            type = DataTypes.guessType(value);
        }
        return Literal.of(type, type.value(value));
    }
}
