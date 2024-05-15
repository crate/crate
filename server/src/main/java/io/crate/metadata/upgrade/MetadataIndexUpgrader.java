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

package io.crate.metadata.upgrade;

import static io.crate.execution.ddl.tables.MappingUtil.DROPPED_COLUMN_NAME_PREFIX;
import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import org.elasticsearch.common.compress.CompressedXContent;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Maps;
import io.crate.server.xcontent.XContentHelper;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import io.crate.Constants;
import org.jetbrains.annotations.VisibleForTesting;

import org.elasticsearch.common.xcontent.XContentType;

public class MetadataIndexUpgrader implements BiFunction<IndexMetadata, IndexTemplateMetadata, IndexMetadata> {

    private final Logger logger;

    public MetadataIndexUpgrader() {
        this.logger = LogManager.getLogger(MetadataIndexUpgrader.class);
    }

    @Override
    public IndexMetadata apply(IndexMetadata indexMetadata,
                               IndexTemplateMetadata indexTemplateMetadata) {
        return createUpdatedIndexMetadata(indexMetadata, indexTemplateMetadata);
    }

    /**
     * Purges any dynamic template from the index metadata because they might be out-dated and the general default
     * template will apply any defaults for all indices.
     */
    private IndexMetadata createUpdatedIndexMetadata(IndexMetadata indexMetadata, @Nullable IndexTemplateMetadata indexTemplateMetadata) {
        return IndexMetadata.builder(indexMetadata)
            .putMapping(
                createUpdatedIndexMetadata(
                    indexMetadata.mapping(),
                    indexMetadata.getIndex().getName(),
                    indexTemplateMetadata
                ))
            .build();
    }

    @VisibleForTesting
    MappingMetadata createUpdatedIndexMetadata(MappingMetadata mappingMetadata, String indexName, @Nullable IndexTemplateMetadata indexTemplateMetadata) {
        if (mappingMetadata == null) { // blobs have no mappingMetadata
            return null;
        }
        Map<String, Object> oldMapping = mappingMetadata.sourceAsMap();
        removeInvalidPropertyGeneratedByDroppingSysCols(oldMapping);
        upgradeColumnPositions(oldMapping, indexTemplateMetadata);
        upgradeIndexColumnMapping(oldMapping, indexTemplateMetadata);
        LinkedHashMap<String, Object> newMapping = new LinkedHashMap<>(oldMapping.size());
        for (Map.Entry<String, Object> entry : oldMapping.entrySet()) {
            String fieldName = entry.getKey();
            Object fieldNode = entry.getValue();
            switch (fieldName) {
                case "dynamic_templates":
                    break; // `dynamic_templates` is no longer supported

                case "_all":
                    break; // `_all` is no longer supported and via CREATE TABLE we always set `_all: {enabled: false}` which is safe to remove.

                default:
                    newMapping.put(fieldName, fieldNode);
            }
        }

        try {
            return new MappingMetadata(Map.of(Constants.DEFAULT_MAPPING_TYPE, newMapping));
        } catch (IOException e) {
            logger.error("Failed to upgrade mapping for index '" + indexName + "'", e);
            return mappingMetadata;
        }
    }

    /**
     * Migrates from "indexed column refers to indices via "copy_to"
     * to "index refers to source columns via new "sources" field.
     *
     * Indices are defined on CREATE TABLE and can only be a part of top-level "properties".
     * Hence,we will be adding sources only into rootMapping.
     *
     * @param rootMapping is the top level properties of the whole table.
     * It's propagated (and mutated) through all calls since we might want to access upper level map
     * when 'copy_to' is part of a deep sub-column.
     *
     * @param currentPath is accumulated FQN. On every call we know only leaf name and 'sources' have to contain FQN to be able to resolve Reference.
     */
    public static boolean addIndexColumnSources(Map<String, Object> rootMapping,
                                                Map<String, Object> currentMapping,
                                                String currentPath) {
        Map<String, Map<String, Object>> propertiesMap = Maps.get(currentMapping, "properties");
        if (propertiesMap == null) {
            return false;
        }
        boolean updated = false;
        for (Map.Entry<String, Map<String, Object>> entry: propertiesMap.entrySet()) {
            String columnName = entry.getKey(); // Not an FQN name, leaf name.
            String columnFQN = currentPath.isEmpty() ? columnName : currentPath + "." + columnName;
            Map<String, Object> columnProperties = entry.getValue();

            // Update index columns
            List<String> indices = Maps.get(columnProperties, "copy_to");
            if (indices != null) {
                updated = true;
                for (String index: indices) {
                    Map<String, Object> indexProps = Maps.get(rootMapping, index);
                    List<String> sources = Maps.get(indexProps, "sources");
                    if (sources == null) {
                        sources = new ArrayList<>();
                        indexProps.put("sources", sources);
                    }
                    sources.add(columnFQN);
                }
                columnProperties.remove("copy_to");
            }

            // There can be nested "properties" field
            // either in "inner" field (ARRAY) or in current column properties (OBJECT).
            String type = Maps.get(columnProperties, "type");
            if (ArrayType.NAME.equals(type)) {
                Map<String, Object> innerMapping = Maps.get(columnProperties, "inner");
                String innerType = Maps.get(innerMapping, "type");
                if (innerType == null || ObjectType.UNTYPED.equals(DataTypes.ofMappingName(innerType))) {
                    updated |= addIndexColumnSources(rootMapping, innerMapping, columnFQN);
                }
            } else {
                // ObjectMapper has logic to skip type if object has "properties" field (i.e has nested sub-column).
                // Hence, type could be null and it indicates that it's an object column.
                if (type == null || ObjectType.UNTYPED.equals(DataTypes.ofMappingName(type))) {
                    updated |= addIndexColumnSources(rootMapping, columnProperties, columnFQN);
                }
            }
        }
        return updated;
    }

    public static boolean removeInvalidPropertyGeneratedByDroppingSysCols(Map<String, Object> defaultMapping) {
        Map<String, Object> properties = Maps.get(defaultMapping, "properties");
        if (properties != null) {
            String droppedUnassigned = DROPPED_COLUMN_NAME_PREFIX + COLUMN_OID_UNASSIGNED;
            if (properties.containsKey(droppedUnassigned)) {
                properties.remove(droppedUnassigned);
                return true;
            }
        }
        return false;
    }

    static void upgradeIndexColumnMapping(Map<String, Object> oldMapping,
                                          @Nullable IndexTemplateMetadata indexTemplateMetadata) {
        addIndexColumnSources(Maps.get(oldMapping, "properties"), oldMapping, "");

        if (indexTemplateMetadata != null) {
            Map<String, Object> parsedTemplateMapping = XContentHelper.convertToMap(
                indexTemplateMetadata.mapping().compressedReference(),
                true,
                XContentType.JSON).map();
            addIndexColumnSources(Maps.get(parsedTemplateMapping, "properties"), parsedTemplateMapping, "");
        }
    }



    /**
     * Fixes index mappings such that all columns contain unique column positions.
     * @param defaultMap An index mapping that may contain duplicates or null positions.
     * @param indexTemplateMetadata if the table is partitioned, it should contain correct column positions.
     */
    private void upgradeColumnPositions(Map<String, Object> defaultMap, @Nullable IndexTemplateMetadata indexTemplateMetadata) {
        if (indexTemplateMetadata != null) {
            populateColumnPositionsFromMapping(defaultMap, indexTemplateMetadata.mapping());
        } else {
            IndexTemplateUpgrader.populateColumnPositions(defaultMap);
        }
    }

    public static void populateColumnPositionsFromMapping(Map<String, Object> mapping, CompressedXContent mappingToReference) {
        Map<String, Object> parsedTemplateMapping = XContentHelper.convertToMap(mappingToReference.compressedReference(), true, XContentType.JSON).map();
        populateColumnPositionsImpl(
            Maps.getOrDefault(mapping, "default", mapping),
            Maps.getOrDefault(parsedTemplateMapping, "default", parsedTemplateMapping)
        );
    }

    // template mappings must contain up-to-date and correct column positions that all relevant index mappings can reference.
    @VisibleForTesting
    static void populateColumnPositionsImpl(Map<String, Object> indexMapping, Map<String, Object> templateMapping) {
        Map<String, Object> indexProperties = Maps.get(indexMapping, "properties");
        if (indexProperties == null) {
            return;
        }
        Map<String, Object> templateProperties = Maps.get(templateMapping, "properties");
        if (templateProperties == null) {
            templateProperties = Map.of();
        }
        for (var e : indexProperties.entrySet()) {
            String key = e.getKey();
            Map<String, Object> indexColumnProperties = (Map<String, Object>) e.getValue();
            Map<String, Object> templateColumnProperties = (Map<String, Object>) templateProperties.get(key);

            if (templateColumnProperties == null) {
                templateColumnProperties = Map.of();
            }
            templateColumnProperties = Maps.getOrDefault(templateColumnProperties, "inner", templateColumnProperties);
            indexColumnProperties = Maps.getOrDefault(indexColumnProperties, "inner", indexColumnProperties);

            Integer templateChildPosition = (Integer) templateColumnProperties.get("position");
            assert templateColumnProperties.containsKey("position") && templateChildPosition != null : "the template mapping is missing column positions";

            // BWC compatibility with nodes < 5.1, position could be NULL if column is created on that nodes
            if (templateChildPosition != null) {
                // since template mapping and index mapping should be consistent, simply override (this will resolve any duplicates in index mappings)
                indexColumnProperties.put("position", templateChildPosition);
            }

            populateColumnPositionsImpl(indexColumnProperties, templateColumnProperties);
        }
    }
}
