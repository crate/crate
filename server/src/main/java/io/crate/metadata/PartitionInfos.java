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

package io.crate.metadata;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import io.crate.metadata.settings.NumberOfReplicas;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

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
        var indexName = indexMetadataEntry.key;
        var indexMetadata = indexMetadataEntry.value;
        PartitionName partitionName = PartitionName.fromIndexOrTemplate(indexName);
        try {
            MappingMetadata mappingMetadata = indexMetadata.mapping();
            if (mappingMetadata == null) {
                LOGGER.trace("Cannot resolve mapping definition of index {}", indexName);
                return null;
            }
            Map<String, Object> mappingMap = mappingMetadata.sourceAsMap();
            Map<String, Object> valuesMap = buildValuesMap(partitionName, mappingMap);
            Settings settings = indexMetadata.getSettings();
            String numberOfReplicas = NumberOfReplicas.getVirtualValue(settings);
            return new PartitionInfo(
                    partitionName,
                    indexMetadata.getNumberOfShards(),
                    numberOfReplicas,
                    IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings),
                    settings.getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, null),
                    indexMetadata.getState() == State.CLOSE,
                    valuesMap,
                    settings);
        } catch (Exception e) {
            LOGGER.trace("error extracting partition infos from index " + indexMetadata, e);
            return null; // must filter on null
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> buildValuesMap(PartitionName partitionName, Map<String, Object> mappingMetadata) throws Exception {
        Map<String, Object> meta = (Map<String, Object>) mappingMetadata.get("_meta");
        if (meta == null) {
            return Map.of();
        }
        Object partitionedBy = meta.get("partitioned_by");
        if (!(partitionedBy instanceof List)) {
            return Map.of();
        }

        List<List<String>> partitionedByColumns = (List<List<String>>) partitionedBy;
        assert partitionedByColumns.size() == partitionName.values().size()
            : "partitionedByColumns size must match partitionName values size";
        Map<String, Object> valuesByColumn = new HashMap<>();
        for (int i = 0; i < partitionedByColumns.size(); i++) {
            var columnAndType = partitionedByColumns.get(i);
            assert columnAndType.size() == 2 : "_meta['partitioned_by'] mapping must contain (columnName, datatype) pairs";
            String dottedColumnName = columnAndType.get(0);
            String sqlFqn = ColumnIdent.fromPath(dottedColumnName).sqlFqn();
            String typeName = columnAndType.get(1);
            DataType<?> type = DataTypes.ofMappingName(typeName);
            Object value = type.implicitCast(partitionName.values().get(i));
            valuesByColumn.put(sqlFqn, value);
        }
        return valuesByColumn;
    }
}
