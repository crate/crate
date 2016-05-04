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

import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.format.SymbolPrinter;
import io.crate.types.DataType;
import org.apache.lucene.util.BytesRef;

import java.util.Locale;

public class ConversionException extends RuntimeException {

    private final static String ERROR_MESSAGE = "cannot cast %s to type %s";

    private String message;

    public ConversionException(Object value, DataType type) {
        super();

        if (value instanceof Symbol) {
            message = String.format(Locale.ENGLISH, ERROR_MESSAGE,
                    SymbolPrinter.INSTANCE.printSimple((Symbol)value), type.toString());
        } else if (value instanceof BytesRef) {
            message = String.format(Locale.ENGLISH, ERROR_MESSAGE,
                    String.format(Locale.ENGLISH, "'%s'", ((BytesRef) value).utf8ToString()), type.toString());
        } else if (value instanceof String) {
            message = String.format(Locale.ENGLISH, ERROR_MESSAGE,
                    String.format(Locale.ENGLISH, "'%s'", value), type.toString());
        } else {
            message = String.format(Locale.ENGLISH, ERROR_MESSAGE,
                    value.toString(), type.toString());
        }
    }

    @Override
    public String getMessage() {
        return message;
    }
}
