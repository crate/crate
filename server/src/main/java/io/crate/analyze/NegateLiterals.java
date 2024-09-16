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

package io.crate.analyze;

import java.math.BigDecimal;

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.Symbols;
import io.crate.types.DataType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.NumericType;
import io.crate.types.ShortType;

public final class NegateLiterals extends SymbolVisitor<Void, Symbol> {

    private static final NegateLiterals INSTANCE = new NegateLiterals();

    private NegateLiterals() {
    }

    public static Symbol negate(Symbol symbol) {
        return symbol.accept(INSTANCE, null);
    }

    @Override
    public Literal<?> visitLiteral(Literal<?> symbol, Void context) {
        Object value = symbol.value();
        if (value == null) {
            return symbol;
        }
        DataType<?> valueType = symbol.valueType();
        switch (valueType.id()) {
            case DoubleType.ID:
                return Literal.ofUnchecked(valueType, (Double) value * -1);
            case FloatType.ID:
                return Literal.ofUnchecked(valueType, (Double) value * -1);
            case ShortType.ID:
                return Literal.ofUnchecked(valueType, (Short) value * -1);
            case IntegerType.ID:
                return Literal.ofUnchecked(valueType, (Integer) value * -1);
            case LongType.ID:
                return Literal.ofUnchecked(valueType, (Long) value * -1);
            case NumericType.ID:
                return Literal.ofUnchecked(valueType, ((BigDecimal) value).negate());
            default:
                throw new UnsupportedOperationException(Symbols.format(
                    "Cannot negate %s. You may need to add explicit type casts", symbol));
        }
    }
}
