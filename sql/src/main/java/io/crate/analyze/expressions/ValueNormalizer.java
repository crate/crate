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
    public Symbol normalizeInputForReference(
            Symbol valueSymbol, Reference reference, ExpressionAnalysisContext context) {

        Literal literal;
        try {
            valueSymbol = normalizer.normalize(valueSymbol);
            assert valueSymbol != null : "valueSymbol must not be null";
            if (valueSymbol.symbolType() != SymbolType.LITERAL) {
                DataType targetType = reference.valueType();
                if (reference instanceof DynamicReference) {
                    targetType = valueSymbol.valueType();
                }
                return ExpressionAnalyzer.castIfNeededOrFail(valueSymbol, targetType, context);
            }
            literal = (Literal) valueSymbol;

            if (reference instanceof DynamicReference) {
                DataType<?> dataType = literal.valueType();
                if (reference.info().columnPolicy() != ColumnPolicy.IGNORED) {
                    raiseIfNestedArray(dataType, reference.info().ident().columnIdent());
                }
                ((DynamicReference) reference).valueType(dataType);
            } else {
                raiseIfNestedArray(literal.valueType(), reference.info().ident().columnIdent());
                literal = Literal.convert(literal, reference.valueType());
            }
        } catch (ConversionException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    String.format("%s can not be cast to \'%s\'", SymbolFormatter.INSTANCE.formatSimple(valueSymbol),
                            reference.valueType().getName()));
        }

        try {
            // 3. if reference is of type object - do special validation
            if (reference.info().type() == DataTypes.OBJECT) {
                @SuppressWarnings("unchecked")
                Map<String, Object> value = (Map<String, Object>) literal.value();
                if (value == null) {
                    return Literal.NULL;
                }
                literal = Literal.newLiteral(normalizeObjectValue(value, reference.info(), true));
            } else if (isObjectArray(reference.info().type())) {
                Object[] value = (Object[]) literal.value();
                if (value == null) {
                    return Literal.NULL;
                }
                literal = Literal.newLiteral(
                        reference.info().type(),
                        normalizeObjectArrayValue(value, reference.info(), true)
                );
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
    private Map<String, Object> normalizeObjectValue(Map<String, Object> value, ReferenceInfo info, boolean forWrite) {
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
                    dynamicReference = ((DocTableInfo)tableInfo).getDynamic(nestedIdent, forWrite);
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
                value.put(entry.getKey(), normalizeObjectValue((Map<String, Object>) entry.getValue(), nestedInfo, forWrite));
            } else if (isObjectArray(nestedInfo.type()) && entry.getValue() instanceof Object[]) {
                value.put(entry.getKey(), normalizeObjectArrayValue((Object[]) entry.getValue(), nestedInfo, forWrite));
            } else {
                value.put(entry.getKey(), normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
        }
        return value;
    }

    private boolean isObjectArray(DataType type) {
        return type.id() == ArrayType.ID && ((ArrayType) type).innerType().id() == ObjectType.ID;
    }

    private Object[] normalizeObjectArrayValue(Object[] value, ReferenceInfo arrayInfo, boolean forWrite) {
        for (Object arrayItem : value) {
            Preconditions.checkArgument(arrayItem instanceof Map, "invalid value for object array type");
            // return value not used and replaced in value as arrayItem is a map that is mutated
            normalizeObjectValue((Map<String, Object>) arrayItem, arrayInfo, forWrite);
        }
        return value;
    }

    private Object normalizePrimitiveValue(Object primitiveValue, ReferenceInfo info) {
        try {
            if (info.type().equals(DataTypes.STRING) && primitiveValue instanceof String) {
                return primitiveValue;
            }
            return info.type().value(primitiveValue);
        } catch (Exception e) {
            throw new ColumnValidationException(info.ident().columnIdent().sqlFqn(),
                    String.format("Invalid %s",
                            info.type().getName()
                    )
            );
        }
    }
}
