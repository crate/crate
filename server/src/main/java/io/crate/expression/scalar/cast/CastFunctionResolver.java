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

package io.crate.expression.scalar.cast;

import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.functions.Signature;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Set;

import static io.crate.metadata.functions.TypeVariableConstraint.typeVariable;
import static io.crate.types.TypeSignature.parseTypeSignature;

public class CastFunctionResolver {

    public static final List<String> CAST_FUNCTION_NAMES = List.of(
        ExplicitCastFunction.NAME, ImplicitCastFunction.NAME, TryCastFunction.NAME);

    public static Symbol generateCastFunction(Symbol sourceSymbol,
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
            var info = FunctionInfo.of(
                modes.contains(CastMode.TRY)
                    ? TryCastFunction.NAME
                    : ExplicitCastFunction.NAME,
                List.of(sourceType, targetType),
                targetType);
            return new Function(
                info,
                Signature
                    .scalar(
                        info.ident().fqnName(),
                        parseTypeSignature("E"),
                        parseTypeSignature("V"),
                        parseTypeSignature("V")
                    ).withTypeVariableConstraints(typeVariable("E"), typeVariable("V")),
                // a literal with a NULL value is passed as an argument
                // to match the method signature
                List.of(sourceSymbol, Literal.of(targetType, null))
            );
        } else {
            var info = FunctionInfo.of(
                ImplicitCastFunction.NAME,
                List.of(sourceType, DataTypes.STRING),
                targetType);
            return new Function(
                info,
                Signature
                    .scalar(
                        info.ident().fqnName(),
                        parseTypeSignature("E"),
                        DataTypes.STRING.getTypeSignature(),
                        DataTypes.UNDEFINED.getTypeSignature())
                    .withTypeVariableConstraints(typeVariable("E")),
                List.of(
                    sourceSymbol,
                    Literal.of(targetType.getTypeSignature().toString())
                )
            );
        }
    }
}
