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

package io.crate.execution.ddl.tables;

import static io.crate.metadata.Reference.buildTree;
import static io.crate.metadata.table.ColumnPolicies.ES_MAPPING_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import org.jetbrains.annotations.Nullable;

import com.carrotsearch.hppc.IntArrayList;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.ColumnPolicies;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

public class MappingUtil {

    public static class AllocPosition {

        private final ToIntFunction<ColumnIdent> getExistingPosition;
        private int nextPosition;

        /**
         * Important: The table must be based on the _latest_ cluster state, retrieved from within a clusterState update routine.
         */
        public static AllocPosition forTable(DocTableInfo table) {
            return new AllocPosition(table.maxPosition(), column -> {
                var reference = table.getReference(column);
                return reference == null ? -1 : reference.position();
            });
        }

        public static AllocPosition forNewTable() {
            return new AllocPosition(0, column -> -1);
        }

        private AllocPosition(int maxPosition, ToIntFunction<ColumnIdent> getExistingPosition) {
            this.getExistingPosition = getExistingPosition;
            this.nextPosition = maxPosition + 1;
        }

        public int position(ColumnIdent column) {
            int position = getExistingPosition.applyAsInt(column);
            if (position < 0) {
                return nextPosition++;
            }
            return position;
        }
    }

    /**
     * This is a singe entry point to creating mapping: adding a column(s), create a table, create a partitioned table (template).
     * @param tableColumnPolicy has default value STRICT if not specified on a table creation.
     * On column addition it's NULL in order to not override an existing value.
     */
    public static Map<String, Object> createMapping(AllocPosition allocPosition,
                                                    List<Reference> columns,
                                                    IntArrayList pKeyIndices,
                                                    Map<String, String> checkConstraints,
                                                    List<List<String>> partitionedBy,
                                                    @Nullable ColumnPolicy tableColumnPolicy,
                                                    @Nullable String routingColumn) {
        HashMap<ColumnIdent, List<Reference>> tree = buildTree(columns);
        Map<String, Map<String, Object>> propertiesMap = createPropertiesMap(allocPosition, null, tree);
        assert propertiesMap != null : "ADD COLUMN mapping can not be null"; // Only intermediate result can be null.

        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> meta = new HashMap<>();
        mergeConstraints(meta, columns, pKeyIndices, checkConstraints);
        if (routingColumn != null) {
            meta.put("routing", routingColumn);
        }

        if (tableColumnPolicy != null) {
            mapping.put(ES_MAPPING_NAME, ColumnPolicies.encodeMappingValue(tableColumnPolicy));
        }

        Map<String, Object> indices = new HashMap<>();
        for (Reference column : columns) {
            if (column instanceof IndexReference indexRef && !indexRef.columns().isEmpty()) {
                indices.put(column.column().name(), Map.of());
            }
        }
        if (indices.isEmpty() == false) {
            meta.put("indices", indices);
        }
        if (partitionedBy.isEmpty() == false) {
            meta.put("partitioned_by", partitionedBy);
        }

        mapping.put("_meta", meta);
        mapping.put("properties", propertiesMap);

        return mapping;
    }

    /**
     * Returns properties map including all nested sub-columns if there any.
     * Format of the top level properties field:
     * {
     *     col1: {
     *        position: some_position
     *        type: some_type
     *        ...
     *
     *        * optional, only for nested objects *
     *        properties: {
     *            nested_col1: {...},
     *            nested_col2: {...},
     *        }
     *     },
     *     col2: {...}
     * }

     */
    @Nullable
    private static Map<String, Map<String, Object>> createPropertiesMap(AllocPosition position,
                                                                        @Nullable ColumnIdent currentNode,
                                                                        HashMap<ColumnIdent, List<Reference>> tree) {
        List<Reference> children = tree.get(currentNode);
        if (children == null) {
            return null;
        }
        HashMap<String, Map<String, Object>> allColumnsMap = new LinkedHashMap<>();
        for (Reference child: children) {
            allColumnsMap.put(child.column().leafName(), addColumnProperties(position, child, tree));
        }
        return allColumnsMap;
    }

    private static Map<String, Object> addColumnProperties(AllocPosition position,
                                                           Reference reference,
                                                           HashMap<ColumnIdent, List<Reference>> tree) {
        Map<String, Object> leafProperties = reference.toMapping(position.position(reference.column()));
        Map<String, Object> properties = leafProperties;
        DataType<?> valueType = reference.valueType();
        while (valueType instanceof ArrayType arrayType) {
            HashMap<String, Object> arrayMapping = new HashMap<>();
            arrayMapping.put("type", "array");
            arrayMapping.put("inner", properties);
            valueType = arrayType.innerType();
            properties = arrayMapping;
        }
        if (valueType.id() == ObjectType.ID) {
            objectMapping(position, leafProperties, reference, tree);
        }
        return properties;
    }

    private static void objectMapping(AllocPosition position,
                                      Map<String, Object> propertiesMap,
                                      Reference reference,
                                      HashMap<ColumnIdent, List<Reference>> tree) {
        propertiesMap.put("dynamic", ColumnPolicies.encodeMappingValue(reference.columnPolicy()));
        Map<String, Map<String, Object>> nestedObjectMap = createPropertiesMap(position, reference.column(), tree);
        if (nestedObjectMap != null) {
            propertiesMap.put("properties", nestedObjectMap);
        }
    }

    @SuppressWarnings("unchecked")
    public static void mergeConstraints(Map<String, Object> meta,
                                         List<Reference> references,
                                         IntArrayList pKeyIndices,
                                         Map<String, String> checkConstraints) {

        // CHECK
        if (checkConstraints.isEmpty() == false) {
            Map<String, String> existingCheckConstraints = (Map<String, String>) meta.get("check_constraints");
            if (existingCheckConstraints == null) {
                existingCheckConstraints = new LinkedHashMap<>();
                meta.put("check_constraints", existingCheckConstraints);
            }
            for (var entry : checkConstraints.entrySet()) {
                String name = entry.getKey();
                String expression = entry.getValue();
                existingCheckConstraints.put(name, expression);
            }
        }

        // PK
        if (pKeyIndices.isEmpty() == false) {
            List<String> primaryKeys = (List<String>) meta.get("primary_keys");
            if (primaryKeys == null) {
                primaryKeys = new ArrayList<>();
                meta.put("primary_keys", primaryKeys);
            }
            for (int i = 0; i < pKeyIndices.size(); i ++) {
                primaryKeys.add(references.get(pKeyIndices.get(i)).column().fqn());
            }
        }

        // Not nulls
        List<String> newNotNulls = references.stream().filter(ref -> !ref.isNullable()).map(ref -> ref.column().fqn()).collect(Collectors.toList());
        if (newNotNulls.isEmpty() == false) {
            Map<String, List<String>> constraints = (Map<String, List<String>>) meta.get("constraints");
            List<String> notNulls = constraints != null ? constraints.get("not_null") : null;
            if (notNulls == null) {
                notNulls = new ArrayList<>();
                var map = new HashMap<>();
                map.put("not_null", notNulls);
                meta.put("constraints", map);
            }
            notNulls.addAll(newNotNulls);
        }

        // Generated expressions
        List<GeneratedReference> newGenExpressions = references.stream()
            .filter(ref -> ref instanceof GeneratedReference)
            .map(ref -> (GeneratedReference) ref)
            .collect(Collectors.toList());
        if (newGenExpressions.isEmpty() == false) {
            Map<String, String> generatedColumns = (Map<String, String>) meta.get("generated_columns");
            if (generatedColumns == null) {
                generatedColumns = new HashMap<>();
                meta.put("generated_columns", generatedColumns);
            }
            for (GeneratedReference genRef: newGenExpressions) {
                generatedColumns.put(genRef.column().fqn(), genRef.formattedGeneratedExpression());
            }
        }
    }
}
