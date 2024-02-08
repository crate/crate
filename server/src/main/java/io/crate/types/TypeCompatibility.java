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

package io.crate.types;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.jetbrains.annotations.Nullable;

public final class TypeCompatibility {

    @Nullable
    public static DataType<?> getCommonType(DataType<?> firstType, DataType<?> secondType) {
        return compatibility(firstType, secondType);
    }

    @Nullable
    private static DataType<?> compatibility(DataType<?> fromType, DataType<?> toType) {
        if (fromType.equals(toType)) {
            return toType;
        }

        if (fromType.equals(UndefinedType.INSTANCE)) {
            return toType;
        }

        if (toType.equals(UndefinedType.INSTANCE)) {
            return fromType;
        }

        String fromTypeBaseName = fromType.getTypeSignature().getBaseTypeName();
        String toTypeBaseName = toType.getTypeSignature().getBaseTypeName();
        if (fromTypeBaseName.equals(toTypeBaseName)) {
            // If given types share the same base, e.g. arrays, parameter types must be compatible.
            if (fromType.getTypeParameters().isEmpty() == false || toType.getTypeParameters().isEmpty() == false) {
                return typeCompatibilityForParametrizedType(fromType, toType);
            }
            return fromType;
        }

        return convertTypeByPrecedence(fromType, toType);
    }

    @Nullable
    private static DataType<?> convertTypeByPrecedence(DataType<?> arg1, DataType<?> arg2) {
        final DataType<?> higherPrecedenceArg;
        final DataType<?> lowerPrecedenceArg;
        if (arg1.precedes(arg2)) {
            higherPrecedenceArg = arg1;
            lowerPrecedenceArg = arg2;
        } else {
            higherPrecedenceArg = arg2;
            lowerPrecedenceArg = arg1;
        }

        final boolean lowerPrecedenceCastable = lowerPrecedenceArg.isConvertableTo(higherPrecedenceArg, false);
        final boolean higherPrecedenceCastable = higherPrecedenceArg.isConvertableTo(lowerPrecedenceArg, false);

        if (lowerPrecedenceCastable) {
            return higherPrecedenceArg;
        } else if (higherPrecedenceCastable) {
            return lowerPrecedenceArg;
        }

        return null;
    }

    @Nullable
    private static DataType<?> typeCompatibilityForParametrizedType(DataType<?> fromType, DataType<?> toType) {
        ArrayList<TypeSignature> commonParameterTypes = new ArrayList<>();
        List<DataType<?>> fromTypeParameters = fromType.getTypeParameters();
        List<DataType<?>> toTypeParameters = toType.getTypeParameters();

        // TODO this block is a bunch of special casing, can we push this into the
        // DataType implementations themselves?

        if (fromType.id() == NumericType.ID && toType.id() == NumericType.ID) {
            var fromPrecision = fromType.numericPrecision();
            if (fromPrecision == null) {
                return toType;
            }
            var toPrecision = toType.numericPrecision();
            if (toPrecision == null) {
                return fromType;
            }
            return fromPrecision > toPrecision ? fromType : toType;
        }

        if (fromTypeParameters.size() != toTypeParameters.size()) {
            if (fromType.id() == ObjectType.ID && toType.id() == ObjectType.ID) {
                return fromTypeParameters.size() > toTypeParameters.size() ? fromType : toType;
            } else if (fromType.id() == StringType.ID && toType.id() == StringType.ID) {
                if (((StringType) fromType).unbound() || ((StringType) toType).unbound()) {
                    return StringType.INSTANCE;
                }
            }
            return null;
        } else if (fromType.id() == toType.id()
                   && fromType.characterMaximumLength() != null
                   && toType.characterMaximumLength() != null) {
            if (fromType.characterMaximumLength() > toType.characterMaximumLength()) {
                return fromType;
            }
            return toType;
        }

        for (int i = 0; i < fromTypeParameters.size(); i++) {
            DataType<?> compatibility = compatibility(fromTypeParameters.get(i), toTypeParameters.get(i));
            if (compatibility == null) {
                return null;
            }
            commonParameterTypes.add(compatibility.getTypeSignature());
        }
        String typeBase = fromType.getTypeSignature().getBaseTypeName();
        return new TypeSignature(typeBase, Collections.unmodifiableList(commonParameterTypes)).createType();
    }
}
