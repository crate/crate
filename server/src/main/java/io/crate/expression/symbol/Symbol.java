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

package io.crate.expression.symbol;

import io.crate.expression.scalar.cast.CastFunctionResolver;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.format.Style;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.Writeable;

public abstract class Symbol implements Writeable {

    public static boolean isLiteral(Symbol symbol, DataType<?> expectedType) {
        return symbol.symbolType() == SymbolType.LITERAL && symbol.valueType().equals(expectedType);
    }

    public abstract SymbolType symbolType();

    public abstract <C, R> R accept(SymbolVisitor<C, R> visitor, C context);

    public abstract DataType<?> valueType();

    /**
     * Casts this Symbol to a new {@link DataType} by wrapping an implicit cast
     * function around it if no {@link CastMode} modes are provided.
     * <p>
     * Subclasses of this class may provide another cast methods.
     *
     * @param targetType The resulting data type after applying the cast
     * @param modes      One of the {@link CastMode} types.
     * @return An instance of {@link Function} which casts this symbol.
     */
    public Symbol cast(DataType<?> targetType, CastMode... modes) {
        if (targetType.equals(valueType())) {
            return this;
        } else if (ArrayType.unnest(targetType).equals(DataTypes.UNTYPED_OBJECT)
                   && valueType().id() == targetType.id()) {
            return this;
        } else if (ArrayType.unnest(targetType).equals(DataTypes.NUMERIC)
                   && valueType().id() == DataTypes.NUMERIC.id()) {
            // Do not cast numerics to unscaled numerics because we do not want to loose precision + scale
            return this;
        }
        return CastFunctionResolver.generateCastFunction(this, targetType, modes);
    }

    @Override
    public final String toString() {
        return toString(Style.UNQUALIFIED);
    }

    public abstract String toString(Style style);
}
