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

import org.jetbrains.annotations.Nullable;

public final class TypeCompatibility {

    @Nullable
    public static DataType<?> getCommonType(DataType<?> type1, DataType<?> type2) {
        if (type1.equals(type2)) {
            return type2;
        }
        if (type1.equals(UndefinedType.INSTANCE)) {
            return type2;
        }
        if (type2.equals(UndefinedType.INSTANCE)) {
            return type1;
        }
        String type1Base = type1.getTypeSignature().getBaseTypeName();
        String type2Base = type2.getTypeSignature().getBaseTypeName();
        if (type1Base.equals(type2Base)) {
            // If given types share the same base, e.g. arrays, parameter types must be compatible.
            if (type1.getTypeParameters().isEmpty() == false || type2.getTypeParameters().isEmpty() == false) {
                try {
                    return DataTypes.merge(type1, type2);
                } catch (IllegalArgumentException ex) {
                    return null;
                }
            }
            return type1;
        }
        return convertTypeByPrecedence(type1, type2);
    }

    @Nullable
    private static DataType<?> convertTypeByPrecedence(DataType<?> type1, DataType<?> type2) {
        final DataType<?> higherPrecedenceArg;
        final DataType<?> lowerPrecedenceArg;
        if (type1.precedes(type2)) {
            higherPrecedenceArg = type1;
            lowerPrecedenceArg = type2;
        } else {
            higherPrecedenceArg = type2;
            lowerPrecedenceArg = type1;
        }
        if (lowerPrecedenceArg.isConvertableTo(higherPrecedenceArg, false)) {
            return higherPrecedenceArg;
        } else if (higherPrecedenceArg.isConvertableTo(lowerPrecedenceArg, false)) {
            return lowerPrecedenceArg;
        }
        return null;
    }
}
