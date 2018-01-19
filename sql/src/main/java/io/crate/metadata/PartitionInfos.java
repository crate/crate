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
import io.crate.analyze.TableParameterInfo;
import io.crate.metadata.doc.DocIndexMetaData;
import io.crate.metadata.doc.PartitionedByMappingExtractor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.StreamSupport;

public class PartitionInfos implements Iterable<PartitionInfo> {

    private final ClusterService clusterService;
    private static final Logger LOGGER = Loggers.getLogger(PartitionInfos.class);

    public PartitionInfos(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public Iterator<PartitionInfo> iterator() {
        // get a fresh one for each iteration
        return StreamSupport.stream(clusterService.state().metaData().indices().spliterator(), false)
            .filter(entry -> IndexParts.isPartitioned(entry.key))
            .map(PartitionInfos::createPartitionInfo)
            .filter(Objects::nonNull)
            .iterator();
    }

    private static PartitionInfo createPartitionInfo(ObjectObjectCursor<String, IndexMetaData> indexMetaDataEntry) {
        PartitionName partitionName = PartitionName.fromIndexOrTemplate(indexMetaDataEntry.key);
        try {
            IndexMetaData indexMetaData = indexMetaDataEntry.value;
            MappingMetaData mappingMetaData = indexMetaData.mapping(Constants.DEFAULT_MAPPING_TYPE);
            Map<String, Object> mappingMap = mappingMetaData.sourceAsMap();
            Map<String, Object> valuesMap = buildValuesMap(partitionName, mappingMetaData);
            BytesRef numberOfReplicas = NumberOfReplicas.fromSettings(indexMetaData.getSettings());
            return new PartitionInfo(
                partitionName,
                indexMetaData.getNumberOfShards(),
                numberOfReplicas,
                DocIndexMetaData.getVersionCreated(mappingMap),
                DocIndexMetaData.getVersionUpgraded(mappingMap),
                DocIndexMetaData.isClosed(indexMetaData, mappingMap, false),
                valuesMap,
                TableParameterInfo.tableParametersFromIndexMetaData(indexMetaData));
        } catch (Exception e) {
            LOGGER.trace("error extracting partition infos from index {}", e, indexMetaDataEntry.key);
            return null; // must filter on null
        }
    }

    private static Map<String, Object> buildValuesMap(PartitionName partitionName, MappingMetaData mappingMetaData) throws Exception {
        int i = 0;
        Map<String, Object> valuesMap = new HashMap<>();
        Iterable<Tuple<ColumnIdent, DataType>> partitionColumnInfoIterable = PartitionedByMappingExtractor.extractPartitionedByColumns(mappingMetaData.sourceAsMap());
        for (Tuple<ColumnIdent, DataType> columnInfo : partitionColumnInfoIterable) {
            String columnName = columnInfo.v1().sqlFqn();
            // produce string type values as string, not bytesref
            Object value = BytesRefs.toString(partitionName.values().get(i));
            if (!columnInfo.v2().equals(DataTypes.STRING)) {
                value = columnInfo.v2().value(value);
            }
            valuesMap.put(columnName, value);
            i++;
        }
        return valuesMap;
    }
}
