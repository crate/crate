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
        var mappingMap = DocIndexMetadata.getMappingMap(indexMetadata);
        Map<String, Object> properties = Maps.get(mappingMap, "properties");
        if (properties != null) {
            reorderPositionsIfNecessary(properties);
            try {
                indexMetadata = IndexMetadata.builder(indexMetadata)
                    .putMapping(new MappingMetadata(Constants.DEFAULT_MAPPING_TYPE,
                                                    Map.of(Constants.DEFAULT_MAPPING_TYPE, mappingMap)))
                    .build();
            } catch (IOException e) {
                logger.error(
                    "Failed to upgrade column positions for index '" + indexMetadata.getIndex().getName() + "'", e);
            }
        }
        return createUpdatedIndexMetadata(indexMetadata);
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

    private void reorderPositionsIfNecessary(Map<String, Object> properties) {
        if (shouldReorder(properties)) {
            reorderPositions(properties, 1);
        }
    }

    private boolean shouldReorder(Map<String, Object> properties) {
        return isDeep(properties, 0, 2) && containsDuplicatePositions(properties, new HashSet<>());
    }

    private boolean containsDuplicatePositions(Map<String, Object> properties, Set<Integer> positions) {

        boolean foundDuplicates = false;
        for (var e : properties.values()) {
            if (foundDuplicates) {
                return true;
            }
            Map<String, Object> m = (Map<String, Object>) e;
            Integer position = (Integer) m.get("position");
            if (position != null) {
                if (positions.contains(position)) {
                    return true;
                } else {
                    positions.add(position);
                }
            }
            if (m.containsKey("properties")) {
                foundDuplicates |= containsDuplicatePositions(Maps.get(m, "properties"), positions);
            }
        }
        return foundDuplicates;
    }

    private boolean isDeep(Map<String, Object> properties, int currentDepth, int targetDepth) {
        if (++currentDepth >= targetDepth) {
            return true;
        }
        boolean isDeep = false;
        for (var e : properties.values()) {
            if (isDeep) {
                return true;
            }
            Map<String, Object> m = (Map<String, Object>) e;
            if (m.containsKey("properties")) {
                isDeep |= isDeep(Maps.get(m, "properties"), currentDepth, targetDepth);
            }
        }
        return isDeep;
    }

    private Integer reorderPositions(Map<String, Object> properties, @Nonnull Integer numFieldsCounter) {

        var sortedProperties = properties.entrySet().stream().sorted(SORT_BY_POSITION_THEN_NAME)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));

        for (var e : sortedProperties.values()) {
            Map<String, Object> m = (Map<String, Object>) e;
            Integer position = (Integer) m.get("position");
            if (position == null) {
                continue;
            }
            if (!position.equals(numFieldsCounter)) {
                m.replace("position", numFieldsCounter);
            }
            numFieldsCounter++;
            if (m.containsKey("properties")) {
                numFieldsCounter = reorderPositions(Maps.get(m, "properties"), numFieldsCounter);
            }
        }
        return numFieldsCounter;
    }
}
