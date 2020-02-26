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

package io.crate.expression.symbol;

import io.crate.expression.scalar.cast.CastFunctionResolver;
import io.crate.planner.ExplainLeaf;
import io.crate.types.DataType;
import io.crate.types.UndefinedType;
import org.elasticsearch.common.io.stream.Writeable;

public abstract class Symbol implements FuncArg, Writeable, ExplainLeaf {

    public static boolean isLiteral(Symbol symbol, DataType<?> expectedType) {
        return symbol.symbolType() == SymbolType.LITERAL && symbol.valueType().equals(expectedType);
    }

    public abstract SymbolType symbolType();

    public abstract <C, R> R accept(SymbolVisitor<C, R> visitor, C context);

    public abstract DataType<?> valueType();

    /**
     * Casts this Symbol to a new {@link DataType} by wrapping a cast function around it.
     * Subclasses of this class may provide another cast methods.
     * @param targetType The resulting data type after applying the cast
     * @return An instance of {@link Function} which casts this symbol.
     */
    public final Symbol cast(DataType<?> targetType) {
        return cast(targetType, false);
    }

    /**
     * Casts this Symbol to a new {@link DataType} by wrapping a cast function around it.
     * Subclasses of this class may provide another cast methods.
     * @param targetType The resulting data type after applying the cast
     * @param tryCast If set to true, will return null the symbol cannot be casted.
     * @return An instance of {@link Function} which casts this symbol.
     */
    public Symbol cast(DataType<?> targetType, boolean tryCast) {
        if (targetType.equals(valueType())) {
            return this;
        }
        return CastFunctionResolver.generateCastFunction(this, targetType, tryCast);
    }

    @Override
    public String toString() {
        return representation();
    }

    /**
     * We only allow casting of
     * {@link Literal},
     * {@link Function},
     * {@link ParameterSymbol},
     * and Symbols whose type is undefined.
     *
     * The reasoning behind this is that we want to avoid
     * query Lucene performance to drop due to casts. This
     * is true for {@link ScopedSymbol}/{@link io.crate.metadata.Reference}.
     */
    @Override
    public boolean canBeCasted() {
        return valueType().id() == UndefinedType.INSTANCE.id();
    }
}
