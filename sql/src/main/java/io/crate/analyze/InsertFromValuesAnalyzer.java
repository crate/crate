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

package io.crate.analyze;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import io.crate.core.StringUtils;
import io.crate.core.collections.StringObjectMaps;
import io.crate.exceptions.ColumnValidationException;
import io.crate.metadata.ColumnIdent;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.InsertFromValues;
import io.crate.sql.tree.ValuesList;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.*;

public class InsertFromValuesAnalyzer extends AbstractInsertAnalyzer<InsertFromValuesAnalysis> {

    @Override
    public Symbol visitInsertFromValues(InsertFromValues node, InsertFromValuesAnalysis context) {
        node.table().accept(this, context);

        handleInsertColumns(node, node.maxValuesLength(), context);

        for (ValuesList valuesList : node.valuesLists()) {
            process(valuesList, context);
        }
        return null;
    }

    @Override
    public Symbol visitValuesList(ValuesList node, InsertFromValuesAnalysis context) {
        if (node.values().size() != context.columns().size()) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Invalid number of values: Got %d columns specified but %d values",
                    context.columns().size(), node.values().size()));
        }
        if (context.table().isPartitioned()) {
            context.newPartitionMap();
        }
        int numPks = context.table().primaryKey().size();
        int numValues = node.values().size();
        if (context.parameterContext().bulkParameters.length > 0) {
            for (int i = 0; i < context.parameterContext().bulkParameters.length; i++) {
                context.parameterContext().setIdx(i);
                addValues(node, context, numPks, numValues);
            }
        } else {
            addValues(node, context, numPks, numValues);
        }
        return null;
    }

    private void addValues(ValuesList node,
                           InsertFromValuesAnalysis context,
                           int numPrimaryKeys,
                           int numValues) {
        List<BytesRef> primaryKeyValues = new ArrayList<>(numPrimaryKeys);
        Map<String, Object> sourceMap = new HashMap<>(numValues);

        int i = 0;
        String routingValue = null;
        for (Expression expression : node.values()) {
            // TODO: instead of doing a type guessing and then a conversion this could
            // be improved by using the dataType from the column Reference as a hint
            Symbol valuesSymbol = process(expression, context);
            // implicit type conversion
            Reference column = context.columns().get(i);
            final ColumnIdent columnIdent = column.info().ident().columnIdent();
            try {
                valuesSymbol = context.normalizeInputForReference(valuesSymbol, column);
            } catch (IllegalArgumentException|UnsupportedOperationException e) {
                throw new ColumnValidationException(column.info().ident().columnIdent().fqn(), e);
            }
            try {
                Object value = ((io.crate.operation.Input)valuesSymbol).value();
                if (value instanceof BytesRef) {
                    value = ((BytesRef) value).utf8ToString();
                }
                if (context.primaryKeyColumnIndices().contains(i)) {
                    int idx = context.table().primaryKey().indexOf(columnIdent);
                    if (idx < 0) {
                        // oh look, one or more nested primary keys!
                        assert value instanceof Map;
                        for (ColumnIdent pkIdent : context.table().primaryKey()) {
                            if (!pkIdent.getRoot().equals(columnIdent)) {
                                continue;
                            }
                            int pkIdx = context.table().primaryKey().indexOf(pkIdent);
                            Object nestedValue = StringObjectMaps.fromMapByPath((Map) value, pkIdent.path());
                            addPrimaryKeyValue(pkIdx, nestedValue, primaryKeyValues);
                        }
                    } else {
                        addPrimaryKeyValue(idx, value, primaryKeyValues);
                    }
                }
                if (i == context.routingColumnIndex()) {
                    routingValue = extractRoutingValue(columnIdent, value, context);
                }
                if (context.partitionedByIndices().contains(i)) {
                    Object rest = processPartitionedByValues(columnIdent, value, context);
                    if (rest != null) {
                        sourceMap.put(columnIdent.name(), rest);
                    }
                } else {
                    sourceMap.put(columnIdent.name(), value);
                }
            } catch (ClassCastException e) {
                // symbol is no input
                throw new ColumnValidationException(columnIdent.name(),
                        String.format("invalid value '%s' in insert statement", valuesSymbol.toString()));
            }
            i++;
        }
        context.sourceMaps().add(sourceMap);
        context.addIdAndRouting(primaryKeyValues, routingValue);
    }

    private void addPrimaryKeyValue(int index, Object value, List<BytesRef> primaryKeyValues) {
        if (value == null) {
            throw new IllegalArgumentException("Primary key value must not be NULL");
        }
        if (primaryKeyValues.size() > index) {
            primaryKeyValues.add(index, BytesRefs.toBytesRef(value));
        } else {
            primaryKeyValues.add(BytesRefs.toBytesRef(value));
        }
    }

    private String extractRoutingValue(ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalysis context) {
        Object clusteredByValue = columnValue;
        ColumnIdent clusteredByIdent = context.table().clusteredBy();
        if (!columnIdent.equals(clusteredByIdent)) {
            // oh my gosh! A nested clustered by value!!!
            assert clusteredByValue instanceof Map;
            clusteredByValue = StringObjectMaps.getByPath(
                    (Map<String, Object>)clusteredByValue,
                    StringUtils.PATH_JOINER.join(clusteredByIdent.path()));
        }
        if (clusteredByValue == null) {
            throw new IllegalArgumentException("Clustered by value must not be NULL");
        }

        return clusteredByValue.toString();
    }

    private Object processPartitionedByValues(final ColumnIdent columnIdent, Object columnValue, InsertFromValuesAnalysis context) {
        int idx = context.table().partitionedBy().indexOf(columnIdent);
        Map<String, String> partitionMap = context.currentPartitionMap();
        if (idx < 0) {
            assert columnValue instanceof Map;
            Map<String, Object> mapValue = (Map<String, Object>) columnValue;
            // hmpf, one or more nested partitioned by columns
            for (ColumnIdent partitionIdent : Iterables.filter(context.table().partitionedBy(), new Predicate<ColumnIdent>() {
                @Override
                public boolean apply(@Nullable ColumnIdent input) {
                    return input != null && input.getRoot().equals(columnIdent);
                }
            })) {
                Object nestedValue = mapValue.remove(StringUtils.PATH_JOINER.join(partitionIdent.path()));
                if (nestedValue instanceof BytesRef) {
                    nestedValue = ((BytesRef) nestedValue).utf8ToString();
                }
                if (partitionMap != null) {
                    partitionMap.put(partitionIdent.fqn(), nestedValue != null ? nestedValue.toString() : null);
                }
            }

            // put the rest into source
            return mapValue;
        } else if (partitionMap != null) {
            partitionMap.put(columnIdent.name(), columnValue != null ? columnValue.toString() : null);
        }
        return null;
    }
}
