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

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;

import io.crate.Constants;
import io.crate.common.annotations.VisibleForTesting;
import org.elasticsearch.cluster.metadata.MetadataMappingService;

import javax.annotation.Nullable;

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
        upgradeColumnPositions(oldMapping, indexTemplateMetadata);
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

    /**
     * Fixes index mappings such that all columns contain unique column positions.
     * @param defaultMap An index mapping that may contain duplicates or null positions.
     * @param indexTemplateMetadata if the table is partitioned, it should contain correct column positions.
     */
    private void upgradeColumnPositions(Map<String, Object> defaultMap, @Nullable IndexTemplateMetadata indexTemplateMetadata) {
        if (indexTemplateMetadata != null) {
            MetadataMappingService.populateColumnPositions(defaultMap, indexTemplateMetadata.getMappings().get("default"));
        } else {
            IndexTemplateUpgrader.populateColumnPositions(defaultMap);
        }
    }
}
