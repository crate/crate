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

package io.crate.exceptions;

import io.crate.expression.symbol.FuncArg;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.types.DataType;

import java.util.Collection;
import java.util.Locale;

public class ConversionException extends IllegalArgumentException {

    public ConversionException(FuncArg source, Collection<DataType> targetTypeCandidates) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot cast `%s` of type `%s` to %s",
            formatFuncArg(source),
            source.valueType(),
            targetTypeCandidates.size() > 1
                ? "any of the types: " + targetTypeCandidates.toString()
                : "type `" + targetTypeCandidates.iterator().next().getName() + "`"
        ));
    }

    public ConversionException(FuncArg source, DataType<?> targetType) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot cast `%s` of type `%s` to %s",
            formatFuncArg(source),
            source.valueType(),
            "type `" + targetType.getName() + "`"
        ));
    }

    public ConversionException(DataType<?> sourceType, DataType<?> targetType) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot cast expressions from type `%s` to type `%s`",
            sourceType.getName(),
            targetType.getName()
        ));
    }

    public ConversionException(Object sourceValue, DataType<?> targetType) {
        super(String.format(
            Locale.ENGLISH,
            "Cannot cast value `%s` to type `%s`",
            sourceValue,
            targetType.getName()
        ));
    }

    private static String formatFuncArg(FuncArg source) {
        return source instanceof Symbol
            ? SymbolPrinter.printUnqualified(((Symbol) source))
            : source.toString();
    }
}
