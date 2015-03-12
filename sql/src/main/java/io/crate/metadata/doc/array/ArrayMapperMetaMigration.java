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

package io.crate.metadata.doc.array;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.crate.Constants;
import io.crate.metadata.ColumnIdent;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.gateway.local.state.meta.LocalGatewayMetaMigrator;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * change old array encoding in _meta.columns
 * to array type in the mapping,
 * remove _meta.columns from crate index mappings
 *
 */
public class ArrayMapperMetaMigration implements LocalGatewayMetaMigrator.LocalGatewayMetaDataMigration {

    private static final ESLogger logger = Loggers.getLogger(ArrayMapperMetaMigration.class);

    @Override
    public MetaData migrateMetaData(MetaData globalMetaData) {
        MetaData.Builder metaBuilder = MetaData.builder(globalMetaData);
        for (ObjectObjectCursor<String, IndexMetaData> cursor : globalMetaData.indices()) {
            MappingMetaData mappingMetaData = cursor.value.mapping(Constants.DEFAULT_MAPPING_TYPE);
            if (mappingMetaData != null) {
                Map<String, Object> mappingMap;
                try {
                    mappingMap = mappingMetaData.sourceAsMap();
                } catch (IOException e) {
                    logger.error("could not parse mapping metadata of index [{}]", e, cursor.key);
                    continue;
                }
                Object meta = mappingMap.get("_meta");
                if (meta != null && meta instanceof Map) {
                    Map<String, Object> metaMap = (Map<String, Object>)meta;
                    Set<ColumnIdent> arrayColumns = getArrayColumns((Map<String, Object>)metaMap.remove("columns"));

                    if (!arrayColumns.isEmpty()) {
                        // migrate mapping columns
                        migrateToNewArrayType(arrayColumns, (Map<String, Object>)mappingMap.get("properties"));

                        try {
                            // this will increase the version of the index metadata
                            metaBuilder.put(
                                    IndexMetaData.builder(cursor.value)
                                            .putMapping(
                                                    Constants.DEFAULT_MAPPING_TYPE,
                                                    XContentFactory.jsonBuilder().map(mappingMap).string()
                                            )
                            );
                        } catch (IOException e) {
                            logger.error("error writing the migrated index metadata for index [{}]", e, cursor.key);
                        }
                    }
                }

            }
        }
        return metaBuilder.build();
    }

    private Set<ColumnIdent> getArrayColumns(@Nullable Map<String, Object> underMetaColumns) {
        Set<ColumnIdent> result;
        if (underMetaColumns == null || underMetaColumns.isEmpty()) {
            result = ImmutableSet.of();
        } else {
            result = new HashSet<>();
            for (Map.Entry<String, Object> topColumn : underMetaColumns.entrySet()) {
                if (topColumn.getValue() != null && topColumn.getValue() instanceof Map) {
                    innerGetArrayColumn((Map<String, Object>) topColumn.getValue(), ColumnIdent.fromPath(topColumn.getKey()), result);
                }
            }
        }
        return result;
    }

    private void innerGetArrayColumn(@Nullable Map<String, Object> metaColumn, ColumnIdent parent, Set<ColumnIdent> result) {
        if (metaColumn != null) {
            Object collectionType = metaColumn.get("collection_type");
            Object properties = metaColumn.get("properties");
            if (collectionType != null && "array".equals(collectionType)) {
                result.add(parent);
            } else if (properties != null && properties instanceof Map) {
                Map<String, Object> propertiesMap = (Map<String, Object>)properties;

                for (Map.Entry<String, Object> child : propertiesMap.entrySet()) {
                    if (child.getValue() != null && child.getValue() instanceof Map) {
                        innerGetArrayColumn((Map<String, Object>) child.getValue(), ColumnIdent.getChild(parent, child.getKey()), result);
                    }
                }
            }
        }
    }

    private Map<String, Object> migrateToNewArrayType(Set<ColumnIdent> idents, @Nullable Map<String, Object> properties) {
        for (ColumnIdent ident : idents) {
            Object columnMapping = getColumnMapping(properties, ident);
            if (columnMapping != null && columnMapping instanceof Map) {
                Map<String, Object> newMapping = ImmutableMap.<String, Object>builder().put("type", "array").put("inner", columnMapping).build();
                overrideExistingColumnMapping(properties, ident, newMapping);
            }
        }
        return properties;
    }

    /**
     * extract column mappings by {@linkplain io.crate.metadata.ColumnIdent}
     *
     * @param mapping the top-level mapping inside ``indexname.type.properties`` as map
     * @param ident the columnident identifying the column to fetch
     * @return the mapping for the column as Object or null if there is no mapping for this column.
     */
    static @Nullable Object getColumnMapping(Map<String, Object> mapping, ColumnIdent ident) {
        Object tmp = null;
        for (String pathElement : Iterables.concat(Arrays.asList(ident.name()), ident.path())) {
            tmp = mapping.get(pathElement);
            if (tmp instanceof Map) {
                Object properties = ((Map) tmp).get("properties");
                if (properties != null && properties instanceof Map)
                    mapping = (Map)properties;
            } else {
                return null;
            }
        }
        return tmp;
    }

    /**
     * override a given column mapping inside an elasticsearch mapping
     * by {@linkplain io.crate.metadata.ColumnIdent}.
     *
     * This method will not add column mappings or map entries.
     *
     * @param mapping the top-level mapping inside ``indexname.type.properties`` as map
     * @param ident the {@linkplain io.crate.metadata.ColumnIdent} identifying the column to fetch
     * @param putMe the object to override the column mapping with
     * @return the mapping that was formerly stored at the column mapping
     *         identified by ident or null if the column mapping did not exist
     *         (in this case the mapping wasn't changed)
     */
    static @org.elasticsearch.common.Nullable Object overrideExistingColumnMapping(Map<String, Object> mapping, ColumnIdent ident, Object putMe) {
        Object tmp = null;
        if (ident.path().isEmpty() && mapping.containsKey(ident.name())) {
            return mapping.put(ident.name(), putMe);
        } else {
            String lastPath = ident.name();
            for (String pathElement : Iterables.concat(Arrays.asList(ident.name()), ident.path())) {
                tmp = mapping.get(pathElement);
                lastPath = pathElement;
                if (tmp != null && tmp instanceof Map) {
                    Object properties = ((Map) tmp).get("properties");
                    if (properties != null && properties instanceof Map)
                        mapping = (Map<String, Object>)properties;
                } else {
                    return null;
                }
            }
            return mapping.put(lastPath, putMe);
        }
    }
}
