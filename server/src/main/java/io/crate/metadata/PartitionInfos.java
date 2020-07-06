/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.metadata;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import io.crate.Constants;
import io.crate.analyze.NumberOfReplicas;
import io.crate.metadata.doc.DocIndexMetadata;
import io.crate.metadata.doc.PartitionedByMappingExtractor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import io.crate.common.collections.Tuple;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

public class PartitionInfos implements Iterable<PartitionInfo> {

    private final ClusterService clusterService;
    private static final Logger LOGGER = LogManager.getLogger(PartitionInfos.class);

    public PartitionInfos(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public Iterator<PartitionInfo> iterator() {
        // get a fresh one for each iteration
        return StreamSupport.stream(clusterService.state().metadata().indices().spliterator(), false)
            .filter(entry -> IndexParts.isPartitioned(entry.key))
            .map(PartitionInfos::createPartitionInfo)
            .filter(Objects::nonNull)
            .iterator();
    }

    private static PartitionInfo createPartitionInfo(ObjectObjectCursor<String, IndexMetadata> indexMetadataEntry) {
        PartitionName partitionName = PartitionName.fromIndexOrTemplate(indexMetadataEntry.key);
        try {
            IndexMetadata indexMetadata = indexMetadataEntry.value;
            MappingMetadata mappingMetadata = indexMetadata.mapping(Constants.DEFAULT_MAPPING_TYPE);
            Map<String, Object> mappingMap = mappingMetadata.sourceAsMap();
            Map<String, Object> valuesMap = buildValuesMap(partitionName, mappingMetadata);
            Settings settings = indexMetadata.getSettings();
            String numberOfReplicas = NumberOfReplicas.fromSettings(settings);
            return new PartitionInfo(
                    partitionName,
                    indexMetadata.getNumberOfShards(),
                    numberOfReplicas,
                    IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings),
                    settings.getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, null),
                    DocIndexMetadata.isClosed(indexMetadata, mappingMap, false),
                    valuesMap,
                    indexMetadata.getSettings());
        } catch (Exception e) {
            LOGGER.trace("error extracting partition infos from index {}", e, indexMetadataEntry.key);
            return null; // must filter on null
        }
    }

    private static Map<String, Object> buildValuesMap(PartitionName partitionName, MappingMetadata mappingMetadata) throws Exception {
        int i = 0;
        Map<String, Object> valuesMap = new HashMap<>();
        Iterable<Tuple<ColumnIdent, DataType>> partitionColumnInfoIterable = PartitionedByMappingExtractor.extractPartitionedByColumns(mappingMetadata.sourceAsMap());
        for (Tuple<ColumnIdent, DataType> columnInfo : partitionColumnInfoIterable) {
            String columnName = columnInfo.v1().sqlFqn();
            Object value = partitionName.values().get(i);
            if (!columnInfo.v2().equals(DataTypes.STRING)) {
                value = columnInfo.v2().value(value);
            }
            valuesMap.put(columnName, value);
            i++;
        }
        return valuesMap;
    }
}
