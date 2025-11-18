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

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.settings.SessionSettings;
import io.crate.sql.SqlFormatter;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public record CastSymbol(Symbol symbol, DataType<?> valueType, CastMode mode) implements Symbol {

    public static <T, U> U implicitCast(T value, DataType<U> type) {
        try {
            return type.implicitCast(value);
        } catch (ConversionException e) {
            throw e;
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ConversionException(value, type);
        }
    }

    public static <T, U> U tryCast(SessionSettings settings, T value, DataType<U> type) {
        try {
            return type.explicitCast(value, settings);
        } catch (ClassCastException | IllegalArgumentException e) {
            throw null;
        }
    }

    public static <T, U> U explicitCast(SessionSettings settings, T value, DataType<U> type) {
        try {
            return type.explicitCast(value, settings);
        } catch (ConversionException e) {
            throw e;
        } catch (ClassCastException | IllegalArgumentException e) {
            throw new ConversionException(value, type);
        }
    }

    public Object eval(SessionSettings settings, Object value) {
        return switch (mode) {
            case EXPLICIT -> explicitCast(settings, value, valueType);
            case IMPLICIT -> implicitCast(value, valueType);
            case TRY -> tryCast(settings, value, valueType);
        };
    }

    public CastSymbol(StreamInput in) throws IOException {
        this(
            Symbol.fromStream(in),
            DataTypes.fromStream(in),
            in.readEnum(CastMode.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(symbol, out);
        DataTypes.toStream(valueType, out);
        out.writeEnum(mode);
    }

    @Override
    public Symbol uncast() {
        return symbol;
    }

    @Override
    public long ramBytesUsed() {
        return symbol.ramBytesUsed();
    }

    @Override
    public Symbol cast(DataType<?> targetType, CastMode... modes) {
        return symbol.cast(targetType, modes);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.CAST;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitCast(this, context);
    }

    @Override
    public String toString() {
        return toString(Style.UNQUALIFIED);
    }

    @Override
    public String toString(Style style) {
        return switch (mode) {
            case IMPLICIT -> symbol.toString(style);
            case EXPLICIT -> "CAST("
                + symbol.toString(style)
                + " AS "
                + SqlFormatter.formatSql(valueType.toColumnType(null))
                + ")";
            case TRY -> "TRY_CAST("
                + symbol.toString(style)
                + " AS "
                + SqlFormatter.formatSql(valueType.toColumnType(null))
                + ")";
        };
    }
}
