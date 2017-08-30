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
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import io.crate.Constants;
import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.TableParameterInfo;
import io.crate.metadata.doc.DocIndexMetaData;
import io.crate.metadata.doc.PartitionedByMappingExtractor;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class PartitionInfos implements Iterable<PartitionInfo> {


    private static final Predicate<ObjectObjectCursor<String, IndexMetaData>> PARTITION_INDICES_PREDICATE = input ->
        input != null
        && IndexParts.isPartitioned(input.key);

    private static final Function<ObjectObjectCursor<String, IndexMetaData>, PartitionInfo> CREATE_PARTITION_INFO_FUNCTION =
        new Function<ObjectObjectCursor<String, IndexMetaData>, PartitionInfo>() {
            @Nullable
            @Override
            public PartitionInfo apply(@Nullable ObjectObjectCursor<String, IndexMetaData> input) {
                assert input != null : "input must not be null";
                PartitionName partitionName = PartitionName.fromIndexOrTemplate(input.key);
                try {
                    MappingMetaData mappingMetaData = input.value.mapping(Constants.DEFAULT_MAPPING_TYPE);
                    Map<String, Object> mappingMap = mappingMetaData.sourceAsMap();
                    Map<String, Object> valuesMap = buildValuesMap(partitionName, mappingMetaData);
                    BytesRef numberOfReplicas = NumberOfReplicas.fromSettings(input.value.getSettings());
                    return new PartitionInfo(
                        partitionName,
                        input.value.getNumberOfShards(),
                        numberOfReplicas,
                        DocIndexMetaData.getRoutingHashFunction(mappingMap),
                        DocIndexMetaData.getVersionCreated(mappingMap),
                        DocIndexMetaData.getVersionUpgraded(mappingMap),
                        DocIndexMetaData.isClosed(input.value, mappingMap, false),
                        valuesMap,
                        TableParameterInfo.tableParametersFromIndexMetaData(input.value));
                } catch (Exception e) {
                    Loggers.getLogger(PartitionInfos.class).trace("error extracting partition infos from index {}", e, input.key);
                    return null; // must filter on null
                }
            }
        };

    private final ClusterService clusterService;

    public PartitionInfos(ClusterService clusterService) {
        this.clusterService = clusterService;
    }

    @Override
    public Iterator<PartitionInfo> iterator() {
        // get a fresh one for each iteration
        return FluentIterable.from(clusterService.state().metaData().indices())
            .filter(PARTITION_INDICES_PREDICATE)
            .transform(CREATE_PARTITION_INFO_FUNCTION)
            .filter(Predicates.notNull())
            .iterator();
    }

    @Nullable
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
