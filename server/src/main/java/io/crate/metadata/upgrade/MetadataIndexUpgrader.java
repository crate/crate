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

import static io.crate.metadata.doc.DocIndexMetadata.SORT_BY_POSITION_THEN_NAME;
import static io.crate.metadata.doc.DocIndexMetadata.furtherColumnProperties;

import java.io.IOException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import io.crate.Constants;
import io.crate.common.annotations.VisibleForTesting;
import io.crate.common.collections.Maps;
import io.crate.metadata.doc.DocIndexMetadata;

public class MetadataIndexUpgrader implements UnaryOperator<IndexMetadata> {

    private final Logger logger;

    public MetadataIndexUpgrader() {
        this.logger = LogManager.getLogger(MetadataIndexUpgrader.class);
    }

    @Override
    public IndexMetadata apply(IndexMetadata indexMetadata) {
        var newIndexMetadata = fixSubColumnPositions(indexMetadata);
        return createUpdatedIndexMetadata(newIndexMetadata);
    }

    /**
     * Purges any dynamic template from the index metadata because they might be out-dated and the general default
     * template will apply any defaults for all indices.
     */
    private IndexMetadata createUpdatedIndexMetadata(IndexMetadata indexMetadata) {
        return IndexMetadata.builder(indexMetadata)
            .putMapping(
                createUpdatedIndexMetadata(
                    indexMetadata.mapping(),
                    indexMetadata.getIndex().getName()
                ))
            .build();
    }

    @VisibleForTesting
    MappingMetadata createUpdatedIndexMetadata(MappingMetadata mappingMetadata, String indexName) {
        if (mappingMetadata == null) { // blobs have no mappingMetadata
            return null;
        }
        Map<String, Object> oldMapping = mappingMetadata.getSourceAsMap();
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
            return new MappingMetadata(
                Constants.DEFAULT_MAPPING_TYPE, Map.of(Constants.DEFAULT_MAPPING_TYPE, newMapping));
        } catch (IOException e) {
            logger.error("Failed to upgrade mapping for index '" + indexName + "'", e);
            return mappingMetadata;
        }
    }

    // See https://github.com/crate/crate/issues/12630 for details
    private IndexMetadata fixSubColumnPositions(IndexMetadata indexMetadata) {
        if (indexMetadata.getCreationVersion().onOrAfter(Version.V_5_0_0)) {
            return indexMetadata;
        }
        var mappingMap = DocIndexMetadata.getMappingMap(indexMetadata);
        Map<String, Object> properties = Maps.get(mappingMap, "properties");
        if (properties != null) {
            if (shouldReorder(properties)) {
                reorderPositions(properties);
                try {
                    return IndexMetadata.builder(indexMetadata)
                        .putMapping(new MappingMetadata(Constants.DEFAULT_MAPPING_TYPE,
                                                        Map.of(Constants.DEFAULT_MAPPING_TYPE, mappingMap))).build();
                } catch (IOException e) {
                    logger.error(
                        "Failed to upgrade column positions for index '" + indexMetadata.getIndex().getName() + "'", e);
                }
            }
        }
        return indexMetadata;
    }

    public static boolean shouldReorder(Map<String, Object> properties) {
        return depthOfNestedSubColumnsExceedsThreshold(properties, 0, 2) &&
               containsDuplicatePositions(properties, new HashSet<>());
    }

    private static boolean containsDuplicatePositions(Map<String, Object> properties, Set<Integer> positions) {

        boolean foundDuplicates = false;
        for (var e : properties.values()) {
            if (foundDuplicates) {
                return true;
            }
            Map<String, Object> columnProperties = (Map<String, Object>) e;
            columnProperties = furtherColumnProperties(columnProperties);
            Integer position = (Integer) columnProperties.get("position");
            if (position != null) {
                if (positions.contains(position)) {
                    return true;
                } else {
                    positions.add(position);
                }
            }
            Map<String, Object> subColumnProperties = Maps.get(columnProperties, "properties");
            if (subColumnProperties != null) {
                foundDuplicates |= containsDuplicatePositions(subColumnProperties, positions);
            }
        }
        return foundDuplicates;
    }

    private static boolean depthOfNestedSubColumnsExceedsThreshold(Map<String, Object> properties, int currentDepth, int threashold) {
        if (++currentDepth >= threashold) {
            return true;
        }
        boolean exceededThreshold = false;
        for (var e : properties.values()) {
            if (exceededThreshold) {
                return true;
            }
            Map<String, Object> columnProperties = (Map<String, Object>) e;
            columnProperties = furtherColumnProperties(columnProperties);
            Map<String, Object> subColumnProperties = Maps.get(columnProperties, "properties");
            if (subColumnProperties != null) {
                exceededThreshold |= depthOfNestedSubColumnsExceedsThreshold(subColumnProperties, currentDepth, threashold);
            }
        }
        return exceededThreshold;
    }

    public static void reorderPositions(Map<String, Object> properties) {
        reorderPositions(properties, 1);
    }

    private static Integer reorderPositions(Map<String, Object> properties, @Nonnull Integer numFieldsCounter) {

        var sortedProperties = properties.entrySet().stream().sorted(SORT_BY_POSITION_THEN_NAME)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        for (var e : sortedProperties.values()) {
            Map<String, Object> columnProperties = (Map<String, Object>) e;
            columnProperties = furtherColumnProperties(columnProperties);
            Integer position = (Integer) columnProperties.get("position");
            if (position == null) {
                Map<String, Object> inner = Maps.get(columnProperties, "inner");
                if (inner != null) {
                    position = (Integer) inner.get("position");
                }
                assert position != null;
            }
            if (!position.equals(numFieldsCounter)) {
                columnProperties.replace("position", numFieldsCounter);
            }
            numFieldsCounter++;
            Map<String, Object> subColumnProperties = Maps.get(columnProperties, "properties");
            if (subColumnProperties != null) {
                numFieldsCounter = reorderPositions(subColumnProperties, numFieldsCounter);
            }
        }
        return numFieldsCounter;
    }
}
