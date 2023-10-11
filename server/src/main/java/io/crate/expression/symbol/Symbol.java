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

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.Writeable;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.scalar.cast.ExplicitCastFunction;
import io.crate.expression.scalar.cast.ImplicitCastFunction;
import io.crate.expression.scalar.cast.TryCastFunction;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.functions.TypeVariableConstraint;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.TypeSignature;

public interface Symbol extends Writeable, Accountable {

    public static boolean isLiteral(Symbol symbol, DataType<?> expectedType) {
        return symbol.symbolType() == SymbolType.LITERAL && symbol.valueType().equals(expectedType);
    }

    public static boolean hasLiteralValue(Symbol symbol, Object value) {
        while (symbol instanceof AliasSymbol alias) {
            symbol = alias.symbol();
        }
        return symbol instanceof Literal<?> literal && Objects.equals(literal.value(), value);
    }

    SymbolType symbolType();

    <C, R> R accept(SymbolVisitor<C, R> visitor, C context);

    DataType<?> valueType();

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
    default Symbol cast(DataType<?> targetType, CastMode... modes) {
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
        return generateCastFunction(this, targetType, modes);
    }

    /**
     * Converts the symbol to a string format where the string output should be able to be parsed to generate the original symbol.
     * NOTE: remember to prefer quoted variants over non quoted when converting.
     */
    String toString(Style style);

    private static Symbol generateCastFunction(Symbol sourceSymbol,
                                              DataType<?> targetType,
                                              CastMode... castModes) {
        var modes = Set.of(castModes);
        assert !modes.containsAll(List.of(CastMode.EXPLICIT, CastMode.IMPLICIT))
            : "explicit and implicit cast modes are mutually exclusive";

        DataType<?> sourceType = sourceSymbol.valueType();
        if (!sourceType.isConvertableTo(targetType, modes.contains(CastMode.EXPLICIT))) {
            throw new ConversionException(sourceType, targetType);
        }

        if (modes.contains(CastMode.TRY) || modes.contains(CastMode.EXPLICIT)) {
            // Currently, it is not possible to resolve a function based on
            // its return type. For instance, it is not possible to generate
            // an object cast function with the object return type which inner
            // types have to be considered as well. Therefore, to bypass this
            // limitation we encode the return type info as the second function
            // argument.
            var name = modes.contains(CastMode.TRY)
                ? TryCastFunction.NAME
                : ExplicitCastFunction.NAME;
            return new Function(
                Signature
                    .scalar(
                        name,
                        TypeSignature.parse("E"),
                        TypeSignature.parse("V"),
                        TypeSignature.parse("V")
                    ).withTypeVariableConstraints(
                        TypeVariableConstraint.typeVariable("E"),
                        TypeVariableConstraint.typeVariable("V")),
                // a literal with a NULL value is passed as an argument
                // to match the method signature
                List.of(sourceSymbol, Literal.of(targetType, null)),
                targetType
            );
        } else {
            return new Function(
                Signature
                    .scalar(
                        ImplicitCastFunction.NAME,
                        TypeSignature.parse("E"),
                        DataTypes.STRING.getTypeSignature(),
                        DataTypes.UNDEFINED.getTypeSignature())
                    .withTypeVariableConstraints(TypeVariableConstraint.typeVariable("E")),
                List.of(
                    sourceSymbol,
                    Literal.of(targetType.getTypeSignature().toString())
                ),
                targetType
            );
        }
    }
}
