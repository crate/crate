/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.expressions;

import com.google.common.base.Preconditions;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.*;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.ConversionException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TableInfo;
import io.crate.types.*;

import java.util.Locale;
import java.util.Map;

public class ValueNormalizer {

    private Schemas schemas;
    private EvaluatingNormalizer normalizer;

    public ValueNormalizer(Schemas schemas, EvaluatingNormalizer normalizer) {
        this.schemas = schemas;
        this.normalizer = normalizer;
    }

    /**
     * normalize and validate given value according to the corresponding {@link io.crate.analyze.symbol.Reference}
     *
     * @param valueSymbol the value to normalize, might be anything from {@link io.crate.metadata.Scalar} to {@link io.crate.analyze.symbol.Literal}
     * @param reference   the reference to which the value has to comply in terms of type-compatibility
     * @return the normalized Symbol, should be a literal
     * @throws io.crate.exceptions.ColumnValidationException
     */
    public Symbol normalizeInputForReference(Symbol valueSymbol, Reference reference) {

        valueSymbol = normalizer.normalize(valueSymbol);
        assert valueSymbol != null : "valueSymbol must not be null";

        DataType<?> targetType = getTargetType(valueSymbol, reference);
        if (!(valueSymbol instanceof Literal)) {
            return ExpressionAnalyzer.castIfNeededOrFail(valueSymbol, targetType);
        }
        Literal literal = (Literal) valueSymbol;
        raiseIfNestedArray(literal.valueType(), reference.info().ident().columnIdent());
        try {
            literal = Literal.convert(literal, reference.valueType());
        } catch (ConversionException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    String.format("%s can not be cast to \'%s\'", SymbolFormatter.INSTANCE.formatSimple(valueSymbol),
                            reference.valueType().getName()));
        }
        Object value = literal.value();
        if (value == null) {
            return literal;
        }
        try {
            if (targetType == DataTypes.OBJECT) {
                //noinspection unchecked
                normalizeObjectValue((Map) value, reference.info());
            } else if (isObjectArray(targetType)) {
                normalizeObjectArrayValue((Object[]) value, reference.info());
            }
        } catch (ConversionException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    SymbolFormatter.INSTANCE.formatTmpl(
                            "\"%s\" has a type that can't be implicitly cast to that of \"%s\" (" + reference.valueType().getName() + ")",
                            literal,
                            reference
                    ));
        }
        return literal;
    }

    private static DataType<?> getTargetType(Symbol valueSymbol, Reference reference) {
        DataType<?> targetType;
        if (reference instanceof DynamicReference) {
            targetType = valueSymbol.valueType();
            ((DynamicReference) reference).valueType(targetType);
            if (reference.info().columnPolicy() != ColumnPolicy.IGNORED) {
                raiseIfNestedArray(targetType, reference.info().ident().columnIdent());
            }
        } else {
            targetType = reference.valueType();
        }
        return targetType;
    }

    /**
     * validate input types to not be nested arrays/collection types
     *
     * @throws ColumnValidationException if input type is a nested array type
     */
    private static void raiseIfNestedArray(DataType dataType, ColumnIdent columnIdent) throws ColumnValidationException {
        if (DataTypes.isCollectionType(dataType) && DataTypes.isCollectionType(((CollectionType) dataType).innerType())) {
            throw new ColumnValidationException(columnIdent.sqlFqn(),
                    String.format(Locale.ENGLISH, "Invalid datatype '%s'", dataType));
        }
    }

    @SuppressWarnings("unchecked")
    private void normalizeObjectValue(Map<String, Object> value, ReferenceInfo info) {
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            ColumnIdent nestedIdent = ColumnIdent.getChild(info.ident().columnIdent(), entry.getKey());
            TableInfo tableInfo = schemas.getTableInfo(info.ident().tableIdent());
            ReferenceInfo nestedInfo = tableInfo.getReferenceInfo(nestedIdent);
            if (nestedInfo == null) {
                if (info.columnPolicy() == ColumnPolicy.IGNORED) {
                    continue;
                }
                DynamicReference dynamicReference = null;
                if (tableInfo instanceof DocTableInfo){
                    dynamicReference = ((DocTableInfo)tableInfo).getDynamic(nestedIdent, true);
                }
                if (dynamicReference == null) {
                    throw new ColumnUnknownException(nestedIdent.sqlFqn());
                }
                DataType type = DataTypes.guessType(entry.getValue(), false);
                if (type == null) {
                    throw new ColumnValidationException(info.ident().columnIdent().sqlFqn(), "Invalid value");
                }
                dynamicReference.valueType(type);
                nestedInfo = dynamicReference.info();
            } else {
                if (entry.getValue() == null) {
                    continue;
                }
            }
            if (nestedInfo.type() == DataTypes.OBJECT && entry.getValue() instanceof Map) {
                normalizeObjectValue((Map<String, Object>) entry.getValue(), nestedInfo);
            } else if (isObjectArray(nestedInfo.type()) && entry.getValue() instanceof Object[]) {
                normalizeObjectArrayValue((Object[]) entry.getValue(), nestedInfo);
            } else {
                entry.setValue(normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
        }
    }

    private static boolean isObjectArray(DataType type) {
        return type.id() == ArrayType.ID && ((ArrayType) type).innerType().id() == ObjectType.ID;
    }

    private void normalizeObjectArrayValue(Object[] value, ReferenceInfo arrayInfo) {
        for (Object arrayItem : value) {
            Preconditions.checkArgument(arrayItem instanceof Map, "invalid value for object array type");
            // return value not used and replaced in value as arrayItem is a map that is mutated

            //noinspection unchecked
            normalizeObjectValue((Map<String, Object>) arrayItem, arrayInfo);
        }
    }

    private static Object normalizePrimitiveValue(Object primitiveValue, ReferenceInfo info) {
        if (info.type().equals(DataTypes.STRING) && primitiveValue instanceof String) {
            return primitiveValue;
        }
        try {
            return info.type().value(primitiveValue);
        } catch (Exception e) {
            throw new ColumnValidationException(info.ident().columnIdent().sqlFqn(),
                    String.format("Invalid %s", info.type().getName())
            );
        }
    }
}
