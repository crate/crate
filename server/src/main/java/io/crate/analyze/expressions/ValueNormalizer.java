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

package io.crate.analyze.expressions;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.ConversionException;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.Scalar;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.protocols.postgres.parser.PgArrayParsingException;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public final class ValueNormalizer {

    private ValueNormalizer() {
    }

    /**
     * normalize and validate given value according to the corresponding {@link io.crate.metadata.Reference}
     *
     * @param valueSymbol the value to normalize, might be anything from {@link Scalar} to {@link io.crate.expression.symbol.Literal}
     * @param reference   the reference to which the value has to comply in terms of type-compatibility
     * @return the normalized Symbol, should be a literal
     * @throws io.crate.exceptions.ColumnValidationException
     */
    public static Symbol normalizeInputForReference(Symbol valueSymbol,
                                                    Reference reference,
                                                    TableInfo tableInfo,
                                                    Function<Symbol, Symbol> normalizer) {
        assert valueSymbol != null : "valueSymbol must not be null";

        DataType<?> targetType = getTargetType(valueSymbol, reference);
        try {
            valueSymbol = normalizer.apply(valueSymbol.cast(reference.valueType()));
        } catch (PgArrayParsingException | ConversionException e) {
            throw new ColumnValidationException(
                reference.column().name(),
                tableInfo.ident(),
                String.format(
                    Locale.ENGLISH,
                    "Cannot cast expression `%s` of type `%s` to `%s`",
                    valueSymbol,
                    valueSymbol.valueType().getName(),
                    reference.valueType().getName()
                )
            );
        }
        if (!(valueSymbol instanceof Literal)) {
            return valueSymbol.cast(targetType);
        }
        Object value = ((Literal<?>) valueSymbol).value();
        if (value == null) {
            return valueSymbol;
        }
        try {
            if (targetType.id() == ObjectType.ID) {
                normalizeObjectValue(uncheckedCast(value), reference, tableInfo);
            } else if (isObjectArray(targetType)) {
                normalizeObjectArrayValue(uncheckedCast(value), reference, tableInfo);
            }
        } catch (PgArrayParsingException | ConversionException e) {
            throw new ColumnValidationException(
                    reference.column().name(),
                    tableInfo.ident(),
                    Symbols.format(
                    "\"%s\" has a type that can't be implicitly cast to that of \"%s\" (" +
                    reference.valueType().getName() + ")",
                    valueSymbol,
                    reference
                ));
        }
        return valueSymbol;
    }

    @SuppressWarnings("unchecked")
    private static final <T> T uncheckedCast(Object value) {
        return (T) value;
    }

    private static DataType<?> getTargetType(Symbol valueSymbol, Reference reference) {
        DataType<?> targetType;
        if (reference instanceof DynamicReference) {
            targetType = valueSymbol.valueType();
            ((DynamicReference) reference).valueType(targetType);
        } else {
            targetType = reference.valueType();
        }
        return targetType;
    }

    @SuppressWarnings("unchecked")
    private static void normalizeObjectValue(Map<String, Object> value, Reference info, TableInfo tableInfo) {
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            ColumnIdent nestedIdent = ColumnIdent.getChildSafe(info.column(), entry.getKey());
            Reference nestedInfo = tableInfo.getReference(nestedIdent);
            if (nestedInfo == null) {
                if (info.columnPolicy() == ColumnPolicy.IGNORED) {
                    continue;
                }
                DynamicReference dynamicReference = null;
                if (tableInfo instanceof DocTableInfo) {
                    dynamicReference = ((DocTableInfo) tableInfo).getDynamic(nestedIdent, true, true);
                }
                if (dynamicReference == null) {
                    throw new ColumnUnknownException(nestedIdent, tableInfo.ident());
                }
                DataType<?> type = DataTypes.guessType(entry.getValue());
                if (type == null) {
                    throw new ColumnValidationException(
                        info.column().sqlFqn(), tableInfo.ident(), "Invalid value");
                }
                dynamicReference.valueType(type);
                nestedInfo = dynamicReference;
            } else {
                if (entry.getValue() == null) {
                    continue;
                }
            }
            if (nestedInfo.valueType().id() == ObjectType.ID && entry.getValue() instanceof Map) {
                normalizeObjectValue((Map<String, Object>) entry.getValue(), nestedInfo, tableInfo);
            } else if (isObjectArray(nestedInfo.valueType()) && entry.getValue() instanceof List) {
                normalizeObjectArrayValue((List<Map<String, Object>>) entry.getValue(), nestedInfo, tableInfo);
            } else {
                entry.setValue(normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
        }
    }

    private static boolean isObjectArray(DataType<?> type) {
        return type instanceof ArrayType<?> arrayType && arrayType.innerType().id() == ObjectType.ID;
    }

    @SuppressWarnings("unchecked")
    private static void normalizeObjectArrayValue(List<Map<String, Object>> values, Reference arrayInfo, TableInfo tableInfo) {
        for (Object value : values) {
            // return value not used and replaced in value as arrayItem is a map that is mutated
            normalizeObjectValue((Map<String, Object>) value, arrayInfo, tableInfo);
        }
    }

    private static Object normalizePrimitiveValue(Object primitiveValue, Reference info) {
        if (info.valueType().equals(DataTypes.STRING) && primitiveValue instanceof String) {
            return primitiveValue;
        }
        try {
            return info.valueType().sanitizeValue(primitiveValue);
        } catch (Exception e) {
            throw new ColumnValidationException(
                info.column().sqlFqn(),
                info.ident().tableIdent(),
                String.format(Locale.ENGLISH, "Invalid %s", info.valueType().getName())
            );
        }
    }
}
