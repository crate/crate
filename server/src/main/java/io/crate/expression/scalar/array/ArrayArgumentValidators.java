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

package io.crate.expression.scalar.array;

import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.List;
import java.util.Locale;

public class ArrayArgumentValidators {

    public static void ensureInnerTypeIsNotUndefined(List<DataType<?>> dataTypes, String functionName) {
        DataType<?> innerType = ((ArrayType<?>) dataTypes.get(0)).innerType();
        if (innerType.equals(DataTypes.UNDEFINED)) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "The inner type of the array argument `%s` function cannot be undefined",
                functionName));
        }
    }

    public static void ensureBothInnerTypesAreNotUndefined(List<DataType<?>> dataTypes, String functionName) {
        DataType<?> innerType0 = ((ArrayType<?>) dataTypes.get(0)).innerType();
        DataType<?> innerType1 = ((ArrayType<?>) dataTypes.get(1)).innerType();

        if (innerType0.equals(DataTypes.UNDEFINED) || innerType1.equals(DataTypes.UNDEFINED)) {
            throw new IllegalArgumentException(
                "One of the arguments of the `" + functionName +
                "` function can be of undefined inner type, but not both");
        }
    }

    public static void ensureSingleArgumentArrayInnerTypeIsNotUndefined(List<DataType<?>> dataTypes) {
        DataType<?> innerType = ((ArrayType<?>) dataTypes.get(0)).innerType();
        if (innerType.equals(DataTypes.UNDEFINED)) {
            throw new IllegalArgumentException(
                "When used with only one argument, the inner type of the array argument cannot be undefined");
        }
    }
}
