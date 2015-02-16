/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.validator;

import com.google.common.base.Function;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.DynamicReference;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.SymbolVisitor;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Singleton
public class Validator {

    @Inject
    public Validator(ReferenceInfos referenceInfos) {
        this.referenceInfos = referenceInfos;
    }

    private final ReferenceInfos referenceInfos;

    private final ValidatorVisitor validatorVisitor = new ValidatorVisitor();

    private class ValidatorVisitor extends SymbolVisitor<Void, Function> {
        @Override
        protected Function visitSymbol(final Symbol symbol, Void context) {
            if (symbol.valueType() != DataTypes.OBJECT) {
                return new Function() {
                    @Nullable
                    @Override
                    public Object apply(Object input) {
                        return symbol.valueType().value(input);
                    }
                };
            }
            return super.visitSymbol(symbol, context);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Function visitReference(final Reference symbol, Void context) {
            if (symbol.valueType() != DataTypes.OBJECT && !isObjectArray(symbol.valueType())) {
                return visitSymbol(symbol, context);
            } else {
                return new Function() {
                    @Nullable
                    @Override
                    public Object apply(@Nullable Object input) {
                        Map<String, Object> obj = (Map<String, Object>) input;
                        normalizeObject(obj, symbol);
                        return obj;
                    }
                };
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeObject(Map<String, Object> value, Reference ref) {
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            ColumnIdent nestedIdent = ColumnIdent.getChild(ref.ident().columnIdent(), entry.getKey());
            TableInfo tableInfo = referenceInfos.getTableInfoUnsafe(ref.info().ident().tableIdent());
            ReferenceInfo nestedInfo = tableInfo.getReferenceInfo(nestedIdent);
            if (nestedInfo == null) {
                if (ref.info().columnPolicy() == ColumnPolicy.IGNORED) {
                    continue;
                }
                DynamicReference dynamicReference = tableInfo.getDynamic(nestedIdent, true);
                if (dynamicReference == null) {
                    throw new ColumnUnknownException(nestedIdent.sqlFqn());
                }
                DataType type = DataTypes.guessType(entry.getValue(), false);
                if (type == null) {
                    throw new ColumnValidationException(ref.info().ident().columnIdent().sqlFqn(), "Invalid value");
                }
                dynamicReference.valueType(type);
                nestedInfo = dynamicReference.info();
            } else {
                if (entry.getValue() == null) {
                    continue;
                }
            }
            if (nestedInfo.type() == DataTypes.OBJECT && entry.getValue() instanceof Map) {
                value.put(entry.getKey(), normalizeObject((Map<String, Object>) entry.getValue(), new Reference(nestedInfo)));
            } else if (isObjectArray(nestedInfo.type()) && entry.getValue() instanceof Object[]) {
                value.put(entry.getKey(), normalizeObjectArrayValue((Object[])entry.getValue(), nestedInfo));
            } else {
                value.put(entry.getKey(), normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
        }
        return value;
    }

    private boolean isObjectArray(DataType type) {
        return type.id() == ArrayType.ID && ((ArrayType)type).innerType().id() == ObjectType.ID;
    }

    private Object normalizePrimitiveValue(Object primitiveValue, ReferenceInfo info) {
        try {
            return info.type().value(primitiveValue);
        } catch (Exception e) {
            throw new ColumnValidationException(info.ident().columnIdent().sqlFqn(),
                    String.format("Invalid %s",
                            info.type().getName()
                    )
            );
        }
    }

    @SuppressWarnings("unchecked")
    private Object[] normalizeObjectArrayValue(Object[] value, ReferenceInfo arrayInfo) {
        for (Object arrayItem : value) {
            Preconditions.checkArgument(arrayItem instanceof Map, "invalid value for object array type");
            // return value not used and replaced in value as arrayItem is a map that is mutated
            normalizeObject((Map<String, Object>) arrayItem, new Reference(arrayInfo));
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    void repairArgs(Object[][] args, List<Symbol> col) {

        List<Function> functions = new ArrayList<>(col.size());
        for (Symbol column : col) {
            functions.add(validatorVisitor.process(column, null));
        }
        for (int i = 0; i < args.length; ++i) {
            for (int j = 0; j < args[i].length; ++j) {
                args[i][j] = functions.get(j).apply(args[i][j]);
            }
        }
    }
}