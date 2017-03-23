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

import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.crate.Constants;
import io.crate.Version;
import io.crate.metadata.PartitionName;
import org.elasticsearch.cluster.metadata.CustomUpgradeService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardStateMetaData;

import javax.annotation.Nullable;
import javax.print.Doc;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Crate specific {@link CustomUpgradeService} which hooks in into ES's {@link org.elasticsearch.gateway.GatewayMetaState}.
 *
 * After upgrading required indices or template metadata, metadata will be marked as upgraded by crate's current {@link Version}.
 */
@Singleton
public class CrateMetaDataUpgradeService extends AbstractComponent implements CustomUpgradeService {

    private final NodeEnvironment nodeEnv;

    @Inject
    public CrateMetaDataUpgradeService(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
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

        Version indexVersion = DocIndexMetaData.getVersionUpgraded(
            mappingMetaData == null ? null :mappingMetaData.sourceAsMap());
        if (mappingMetaData == newMappingMetaData && (indexVersion == null || indexVersion.id > 1010099)) {
            return indexMetaData;
        }

        IndexMetaData.Builder builder = IndexMetaData.builder(indexMetaData);
        // Use indexName as a new UUID for the index due to a former bug that resulted in
        // partitions with the same index UUID when created during a bulk insert.
        if (PartitionName.isPartition(indexMetaData.getIndex())) {
            String indexName = indexMetaData.getIndex();
            String newUUID = indexName;
            builder.settings(Settings.builder()
                .put(indexMetaData.getSettings())
                .put(IndexMetaData.SETTING_INDEX_UUID, newUUID));
            logger.info("new UUID={} was set for index={}", newUUID, indexName);
            upgradeShardMetaData(nodeEnv, indexName, newUUID, logger);

            // Mark as Upgraded
            if (mappingMetaData == newMappingMetaData) {
                Map<String, Object> newMappingMap = newMappingMetaData.sourceAsMap();
                markAsUpgraded(newMappingMap);
                Map<String, Object> typeAndMapping = new HashMap<>(1);
                typeAndMapping.put(Constants.DEFAULT_MAPPING_TYPE, newMappingMap);
                newMappingMetaData = new MappingMetaData(typeAndMapping);
            }
        }

        if (mappingMetaData != newMappingMetaData) {
            logger.info("upgraded mapping of index={}", indexMetaData.getIndex());
            builder.removeMapping(Constants.DEFAULT_MAPPING_TYPE).putMapping(newMappingMetaData);
        }
        return builder.build();
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
                : new HashMap<>(mappingMap);

            Map<String, Object> newMetaMap = (Map<String, Object>) newMappingMap.get("_meta");
            if (newMetaMap == null) {
                newMetaMap = new HashMap<>(1);
                newMappingMap.put("_meta", newMetaMap);
            }
            newMetaMap.put(DocIndexMetaData.SETTING_ROUTING_HASH_FUNCTION, routingHashFunction);
            markAsUpgraded(newMappingMap);

            Map<String, Object> typeAndMapping = new HashMap<>(1);
            typeAndMapping.put(Constants.DEFAULT_MAPPING_TYPE, newMappingMap);
            return new MappingMetaData(typeAndMapping);
        }
        return mappingMetaData;
    }

    private static void markAsUpgraded(Map<String, Object> mappingMap) throws IOException {
        assert mappingMap != null : "mapping metadata must not be null to be marked as upgraded";
        Map<String, Object> newMetaMap = (Map<String, Object>) mappingMap.get("_meta");
        DocIndexMetaData.putVersionToMap(newMetaMap, Version.Property.UPGRADED, Version.CURRENT);
    }

    private static void upgradeShardMetaData(NodeEnvironment nodeEnv,
                                             String indexName,
                                             String indexUUID,
                                             ESLogger logger) throws IOException {
        for (ShardId shardId : findAllShardIds(nodeEnv.indexPaths(new Index(indexName)))) {
            try (ShardLock lock = nodeEnv.shardLock(shardId, 0)) {

                final Path[] paths = nodeEnv.availableShardPaths(shardId);
                final ShardStateMetaData loaded = ShardStateMetaData.FORMAT.loadLatestState(logger, paths);
                if (loaded == null) {
                    throw new IllegalStateException("[" + shardId + "] no shard state found in any of: " +
                                                    Arrays.toString(paths) +
                                                    " please check and remove them if possible");
                }

                for (Path path : paths) {
                    ShardStateMetaData newShardStateMetaData = new ShardStateMetaData(
                        loaded.version,
                        loaded.primary,
                        indexUUID);
                    ShardStateMetaData.FORMAT.write(newShardStateMetaData, newShardStateMetaData.version, path);
                    logger.trace("new UUID={} was set for shard={} of index={} in path={}",
                        indexUUID,
                        shardId,
                        indexName,
                        path);
                }
            }
        }
    }

    private static Set<ShardId> findAllShardIds(Path... locations) throws IOException {
        Set<ShardId> shardIds = Sets.newHashSet();
        for (final Path location : locations) {
            if (Files.isDirectory(location)) {
                shardIds.addAll(findAllShardsForIndex(location));
            }
        }
        return shardIds;
    }

    private static Set<ShardId> findAllShardsForIndex(Path indexPath) throws IOException {
        Set<ShardId> shardIds = Sets.newHashSet();
        if (Files.isDirectory(indexPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                String currentIndex = indexPath.getFileName().toString();
                for (Path shardPath : stream) {
                    if (Files.isDirectory(shardPath)) {
                        Integer shardId = Ints.tryParse(shardPath.getFileName().toString());
                        if (shardId != null) {
                            ShardId id = new ShardId(currentIndex, shardId);
                            shardIds.add(id);
                        }
                    }
                }
            }
        }
        return shardIds;
    }
}
