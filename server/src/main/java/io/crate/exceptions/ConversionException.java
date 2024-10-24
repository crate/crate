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

package io.crate.exceptions;

import java.util.Arrays;
import java.util.Locale;

import io.crate.expression.symbol.Symbol;
import io.crate.types.DataType;

public class ConversionException extends IllegalArgumentException {

    public ConversionException(Symbol source, DataType<?> targetType) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot cast `%s` of type `%s` to %s",
            source,
            source.valueType(),
            "type `" + targetType + "`"
        ));
    }

    public ConversionException(DataType<?> sourceType, DataType<?> targetType) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot cast expressions from type `%s` to type `%s`",
            sourceType,
            targetType
        ));
    }

    public ConversionException(Object sourceValue, DataType<?> targetType) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot cast value `%s` to type `%s`",
            sourceValue.getClass().isArray() ? Arrays.deepToString((Object[]) sourceValue) : sourceValue,
            targetType
        ));
    }

    public static ConversionException forObjectChild(String key, Object sourceValue, DataType<?> targetType) {
        return new ConversionException(String.format(
            Locale.ENGLISH,
            "Cannot cast object element `%s` with value `%s` to type `%s`",
            key,
            sourceValue,
            targetType
        ));
    }

    private ConversionException(String message) {
        super(message);
    }
}
