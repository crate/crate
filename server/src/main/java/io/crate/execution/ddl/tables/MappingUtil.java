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
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.ToIntFunction;

import org.elasticsearch.cluster.metadata.RelationMetadata;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

public final class MappingUtil {

    public static final String DROPPED_COLUMN_NAME_PREFIX = "_dropped_";

    private MappingUtil() {}

    public static class AllocPosition {

        private final ToIntFunction<ColumnIdent> getExistingPosition;
        private int nextPosition;

        /**
         * Important: The table must be based on the _latest_ cluster state, retrieved from within a clusterState update routine.
         */
        public static AllocPosition forTable(TableInfo table) {
            return new AllocPosition(table.maxPosition(), column -> {
                var reference = table.getReference(column);
                if (reference == null) {
                    for (Reference droppedColumn : table.droppedColumns()) {
                        if (droppedColumn.column().equals(column)) {
                            return droppedColumn.position();
                        }
                    }
                    return -1;
                }
                return reference.position();
            });
        }

        public static AllocPosition forTable(RelationMetadata.Table table) {
            int maxPosition = 0;
            HashMap<ColumnIdent, Integer> colPositions = new HashMap<>();
            HashMap<ColumnIdent, Integer> droppedColPositions = new HashMap<>();

            for (Reference reference : table.columns()) {
                if (reference.isDropped()) {
                    droppedColPositions.put(reference.column(), reference.position());
                } else {
                    colPositions.put(reference.column(), reference.position());
                }
                if (reference.position() > maxPosition) {
                    maxPosition = reference.position();
                }
            }
            return new AllocPosition(maxPosition, column -> {
                var position = colPositions.get(column);
                if (position == null) {
                    position = droppedColPositions.get(column);
                }
                return Objects.requireNonNullElse(position, -1);
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
     * This is a single entry point to creating mapping: adding a column(s), create a table, create a partitioned table (template).
     * @param tableColumnPolicy has default value STRICT if not specified on a table creation.
     * On column addition it's NULL in order to not override an existing value.
     *
     */
    public static Map<String, Object> createMapping(AllocPosition allocPosition,
                                                    @Nullable String pkConstraintName,
                                                    List<Reference> columns,
                                                    List<ColumnIdent> primaryKeys,
                                                    Map<String, String> checkConstraints,
                                                    List<ColumnIdent> partitionedBy,
                                                    @Nullable ColumnPolicy tableColumnPolicy,
                                                    @Nullable ColumnIdent routingColumn) {

        HashMap<ColumnIdent, List<Reference>> tree = buildTree(columns);
        Map<String, Map<String, Object>> propertiesMap = toProperties(allocPosition, null, tree);
        assert propertiesMap != null : "ADD COLUMN mapping can not be null"; // Only intermediate result can be null.

        Map<String, Object> mapping = new HashMap<>();
        Map<String, Object> meta = new HashMap<>();
        if (pkConstraintName != null) {
            meta.put("pk_constraint_name", pkConstraintName);
        }
        mergeConstraints(meta, columns, primaryKeys, checkConstraints);
        if (routingColumn != null) {
            meta.put("routing", routingColumn.fqn());
        }

        if (tableColumnPolicy != null) {
            mapping.put(ColumnPolicy.MAPPING_KEY, tableColumnPolicy.toMappingValue());
        }

        Map<String, Object> indices = new HashMap<>();
        for (Reference column : columns) {
            if (column instanceof IndexReference indexRef && !indexRef.columns().isEmpty()) {
                indices.put(column.storageIdent(), Map.of());
            }
        }
        if (indices.isEmpty() == false) {
            meta.put("indices", indices);
        }
        if (partitionedBy.isEmpty() == false) {
            List<List<String>> pColumns = Lists.map(partitionedBy, pColumn -> {
                int refIdx = Reference.indexOf(columns, pColumn);
                Reference pRef = columns.get(refIdx);
                return toPartitionMapping(pRef);
            });
            meta.put("partitioned_by", pColumns);
        }

        mapping.put("_meta", meta);
        mapping.put("properties", propertiesMap);

        return mapping;
    }

    private static List<String> toPartitionMapping(Reference ref) {
        String fqn = ref.column().fqn();
        String typeMappingName = DataTypes.esMappingNameFrom(ref.valueType().id());
        return List.of(fqn, typeMappingName);
    }

    /**
     * Creates the "properties" part of a mapping.
     * Includes all nested sub-columns.
     *
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
    public static Map<String, Map<String, Object>> toProperties(AllocPosition allocPosition,
                                                                HashMap<ColumnIdent, List<Reference>> tree) {
        return toProperties(allocPosition, null, tree);
    }

    /**
     * See {@link #toProperties(AllocPosition, HashMap)}
     */
    @Nullable
    private static Map<String, Map<String, Object>> toProperties(AllocPosition position,
                                                                 @Nullable ColumnIdent currentNode,
                                                                 HashMap<ColumnIdent, List<Reference>> tree) {
        List<Reference> children = tree.get(currentNode);
        if (children == null) {
            return null;
        }
        HashMap<String, Map<String, Object>> allColumnsMap = new LinkedHashMap<>();
        for (Reference child: children) {
            allColumnsMap.put(mappingKey(child), addColumnProperties(position, child, tree));
        }
        return allColumnsMap;
    }

    private static String mappingKey(Reference reference) {
        if (reference.isDropped()) {
            assert reference.oid() != COLUMN_OID_UNASSIGNED : "Only columns with assigned OID-s can be dropped";
            return DROPPED_COLUMN_NAME_PREFIX + reference.oid();
        } else {
            return reference.column().leafName();
        }
    }

    private static Map<String, Object> addColumnProperties(AllocPosition position,
                                                           Reference reference,
                                                           HashMap<ColumnIdent, List<Reference>> tree) {
        Map<String, Object> leafProperties = reference.toMapping(position.position(reference.column()));
        Map<String, Object> properties = leafProperties;
        DataType<?> valueType = reference.valueType();
        while (valueType instanceof ArrayType<?> arrayType) {
            HashMap<String, Object> arrayMapping = new HashMap<>();
            arrayMapping.put("type", "array");
            arrayMapping.put("inner", properties);
            valueType = arrayType.innerType();
            properties = arrayMapping;
        }
        if (valueType instanceof ObjectType objectType) {
            objectMapping(position, leafProperties, objectType, reference.column(), tree);
        }
        return properties;
    }

    private static void objectMapping(AllocPosition position,
                                      Map<String, Object> propertiesMap,
                                      ObjectType objectType,
                                      ColumnIdent columnIdent,
                                      HashMap<ColumnIdent, List<Reference>> tree) {
        propertiesMap.put(ColumnPolicy.MAPPING_KEY, (objectType.columnPolicy().toMappingValue()));
        Map<String, Map<String, Object>> nestedObjectMap = toProperties(position, columnIdent, tree);
        if (nestedObjectMap != null) {
            propertiesMap.put("properties", nestedObjectMap);
        }
    }

    @SuppressWarnings("unchecked")
    public static void mergeConstraints(Map<String, Object> meta,
                                        List<Reference> references,
                                        List<ColumnIdent> primaryKeys,
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
        if (primaryKeys.isEmpty() == false) {
            List<String> fqPrimaryKeys = (List<String>) meta.get("primary_keys");
            if (fqPrimaryKeys == null) {
                fqPrimaryKeys = new ArrayList<>();
                meta.put("primary_keys", fqPrimaryKeys);
            }
            for (ColumnIdent primaryKey : primaryKeys) {
                fqPrimaryKeys.add(primaryKey.fqn());
            }
        }

        // Not nulls
        ArrayList<String> newNotNulls = new ArrayList<>();
        for (int i = 0; i < references.size(); i++) {
            Reference ref = references.get(i);
            // primary keys are implicitly null and not explicitly stored within not null constraints
            if (!ref.isNullable() && !primaryKeys.contains(ref.column())) {
                newNotNulls.add(ref.column().fqn());
            }
        }
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
            .filter(ref -> ref instanceof GeneratedReference && !ref.isDropped())
            .map(ref -> (GeneratedReference) ref)
            .toList();
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
