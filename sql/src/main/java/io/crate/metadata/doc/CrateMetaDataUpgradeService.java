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

package io.crate.metadata.doc;

import io.crate.Constants;
import io.crate.metadata.PartitionName;
import org.elasticsearch.cluster.metadata.CustomUpgradeService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Crate specific {@link CustomUpgradeService} which hooks in into ES's {@link org.elasticsearch.gateway.GatewayMetaState}.
 */
@Singleton
public class CrateMetaDataUpgradeService extends AbstractComponent implements CustomUpgradeService {

    @Inject
    public CrateMetaDataUpgradeService(Settings settings) {
        super(settings);
    }

    @Override
    public IndexMetaData upgradeIndexMetaData(IndexMetaData indexMetaData) {
        try {
            return upgradeIndexMapping(indexMetaData);
        } catch (Exception e) {
            logger.error("index={} could not be upgraded", indexMetaData.getIndex());
            throw new RuntimeException(e);
        }
    }

    @Override
    public IndexTemplateMetaData upgradeIndexTemplateMetaData(IndexTemplateMetaData indexTemplateMetaData) {
        try {
            return upgradeTemplateMapping(indexTemplateMetaData);
        } catch (Exception e) {
            logger.error("template={} could not be upgraded", indexTemplateMetaData.getName());
            throw new RuntimeException(e);
        }
    }

    private IndexMetaData upgradeIndexMapping(IndexMetaData indexMetaData) throws IOException {
        MappingMetaData mappingMetaData = indexMetaData.mapping(Constants.DEFAULT_MAPPING_TYPE);
        MappingMetaData newMappingMetaData = saveRoutingHashFunctionToMapping(
            mappingMetaData,
            indexMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION));
        if (mappingMetaData != newMappingMetaData) {
            logger.info("upgraded mapping of index={}", indexMetaData.getIndex());
            return IndexMetaData.builder(indexMetaData)
                .removeMapping(Constants.DEFAULT_MAPPING_TYPE)
                .putMapping(newMappingMetaData)
                .build();
        }
        return indexMetaData;
    }

    private IndexTemplateMetaData upgradeTemplateMapping(IndexTemplateMetaData indexTemplateMetaData) throws IOException {
        // we only want to upgrade partition table related templates
        if (PartitionName.isPartition(indexTemplateMetaData.getTemplate()) == false) {
            return indexTemplateMetaData;
        }
        MappingMetaData mappingMetaData = new MappingMetaData(indexTemplateMetaData.getMappings().get(Constants.DEFAULT_MAPPING_TYPE));
        MappingMetaData newMappingMetaData = saveRoutingHashFunctionToMapping(
            mappingMetaData,
            indexTemplateMetaData.getSettings().get(IndexMetaData.SETTING_LEGACY_ROUTING_HASH_FUNCTION));
        if (mappingMetaData != newMappingMetaData) {
            logger.info("upgraded mapping of template={}", indexTemplateMetaData.getName());
            return new IndexTemplateMetaData.Builder(indexTemplateMetaData)
                .removeMapping(Constants.DEFAULT_MAPPING_TYPE)
                .putMapping(Constants.DEFAULT_MAPPING_TYPE, newMappingMetaData.source())
                .build();
        }
        return indexTemplateMetaData;
    }

    private MappingMetaData saveRoutingHashFunctionToMapping(@Nullable MappingMetaData mappingMetaData,
                                                             @Nullable String routingHashFunction) throws IOException {
        Map<String, Object> mappingMap = null;
        if (mappingMetaData != null) {
            mappingMap = mappingMetaData.sourceAsMap();
        }
        String hashFunction = DocIndexMetaData.getRoutingHashFunction(mappingMap);
        if (hashFunction == null) {
            if (routingHashFunction == null) {
                routingHashFunction = DocIndexMetaData.DEFAULT_ROUTING_HASH_FUNCTION;
            }
            // create new map, existing one can be immutable
            Map<String, Object> newMappingMap = mappingMap == null
                ? new HashMap<>(1)
                : MapBuilder.newMapBuilder(mappingMap).map();

            Map<String, Object> newMetaMap = (Map<String, Object>) newMappingMap.get("_meta");
            if (newMetaMap == null) {
                newMetaMap = new HashMap<>(1);
                newMappingMap.put("_meta", newMetaMap);
            }
            newMetaMap.put(DocIndexMetaData.SETTING_ROUTING_HASH_FUNCTION, routingHashFunction);

            Map<String, Object> typeAndMapping = new HashMap<>(1);
            typeAndMapping.put(Constants.DEFAULT_MAPPING_TYPE, newMappingMap);
            return new MappingMetaData(typeAndMapping);
        }
        return mappingMetaData;
    }
}
