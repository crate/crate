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

package io.crate.expression.reference;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.crate.common.collections.Maps;
import io.crate.exceptions.ConversionException;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.ValueExtractors;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

/**
 * ReferenceResolver implementation which can be used to retrieve {@link CollectExpression}s to extract values from {@link Doc}
 *
 */
public final class DocRefResolver implements ReferenceResolver<CollectExpression<Doc, ?>> {

    private final List<ColumnIdent> partitionedByColumns;

    public DocRefResolver(List<ColumnIdent> partitionedByColumns) {
        this.partitionedByColumns = partitionedByColumns;
    }

    @Override
    public CollectExpression<Doc, ?> getImplementation(Reference ref) {
        ColumnIdent columnIdent = ref.column();
        String fqn = columnIdent.fqn();
        switch (fqn) {
            case DocSysColumns.Names.VERSION:
                return forFunction(Doc::getVersion);

            case DocSysColumns.Names.SEQ_NO:
                return forFunction(Doc::getSeqNo);

            case DocSysColumns.Names.PRIMARY_TERM:
                return forFunction(Doc::getPrimaryTerm);

            case DocSysColumns.Names.ID:
                return NestableCollectExpression.forFunction(Doc::getId);

            case DocSysColumns.Names.DOCID:
                return forFunction(Doc::docId);

            case DocSysColumns.Names.RAW:
                return forFunction(Doc::getRaw);

            case DocSysColumns.Names.DOC:
                return forFunction(Doc::getSource);

            default:
                final ColumnIdent column = columnIdent.name().equals(DocSysColumns.Names.DOC)
                    ? columnIdent.shiftRight()
                    : columnIdent;
                for (int i = 0; i < partitionedByColumns.size(); i++) {
                    var pColumn = partitionedByColumns.get(i);
                    if (pColumn.equals(column)) {
                        final int idx = i;
                        return forFunction(
                            getResp -> ref.valueType().implicitCast(
                                PartitionName.fromIndexOrTemplate(getResp.getIndex()).values().get(idx))
                        );
                    } else if (pColumn.isChildOf(column)) {
                        final int idx = i;
                        return forFunction(response -> {
                            if (response == null) {
                                return null;
                            }
                            var partitionName = PartitionName.fromIndexOrTemplate(response.getIndex());
                            var partitionValue = partitionName.values().get(idx);
                            var source = response.getSource();
                            Maps.mergeInto(source, pColumn.name(), pColumn.path(), partitionValue);
                            Object value = ValueExtractors.fromMap(source, column);
                            return ref.valueType().implicitCast(value);
                        });
                    }
                }

                return forFunction(response -> {
                    if (response == null) {
                        return null;
                    }
                    try {
                        return ref.valueType().sanitizeValue(ValueExtractors.fromMap(response.getSource(), column));
                    } catch (ClassCastException | ConversionException e) {
                        // due to a bug: https://github.com/crate/crate/issues/13990
                        Object value = ValueExtractors.fromMap(response.getSource(), column);
                        return replaceArraysWithNull(value, ref.valueType(), e);
                    }
                });

        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static Object replaceArraysWithNull(Object value, DataType<?> valueType, RuntimeException e)
        throws RuntimeException {

        if (value instanceof List<?> list && list.stream().allMatch(Objects::isNull) &&
            valueType.id() != ArrayType.ID) {
            return null;
        } else if (value instanceof Map<?, ?> valueMap) {
            Map newMap = new HashMap<>(valueMap.size());
            for (var entry : valueMap.entrySet()) {
                Object entryKey = entry.getKey();
                Object entryValue = entry.getValue();
                DataType<?> innerType = ((ObjectType) valueType).innerType((String) entryKey);
                newMap.put(entryKey, replaceArraysWithNull(entryValue, innerType, e));
            }
            return newMap;
        } else {
            throw e;
        }
    }
}
