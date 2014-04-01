/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.metadata.doc;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import io.crate.Constants;
import io.crate.DataType;
import io.crate.exceptions.TableAliasSchemaException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class DocIndexMetaData {

    private static final String ID = "_id";
    private final IndexMetaData metaData;


    private final MappingMetaData defaultMappingMetaData;
    private final Map<String, Object> defaultMappingMap;

    private final ImmutableList.Builder<ReferenceInfo> columnsBuilder = ImmutableList.builder();

    // columns should be ordered
    private final ImmutableMap.Builder<ColumnIdent, ReferenceInfo> referencesBuilder = ImmutableSortedMap.naturalOrder();
    private final ImmutableList.Builder<ReferenceInfo> partitionedByColumnsBuilder = ImmutableList.builder();

    private final TableIdent ident;
    private final int numberOfShards;
    private final BytesRef numberOfReplicas;
    private Map<String, Object> metaMap;
    private Map<String, Object> metaColumnsMap;
    private Map<String, Object> indicesMap;
    private List<List<String>> partitionedByList;
    private ImmutableList<ReferenceInfo> columns;
    private ImmutableList<ReferenceInfo> partitionedByColumns;
    private ImmutableMap<ColumnIdent, ReferenceInfo> references;
    private ImmutableList<String> primaryKey;
    private String routingCol;
    private ImmutableList<String> partitionedBy;
    private final boolean isAlias;
    private final Set<String> aliases;
    private boolean hasAutoGeneratedPrimaryKey = false;

    private final static ImmutableMap<String, DataType> dataTypeMap = ImmutableMap.<String, DataType>builder()
            .put("date", DataType.TIMESTAMP)
            .put("string", DataType.STRING)
            .put("boolean", DataType.BOOLEAN)
            .put("byte", DataType.BYTE)
            .put("short", DataType.SHORT)
            .put("integer", DataType.INTEGER)
            .put("long", DataType.LONG)
            .put("float", DataType.FLOAT)
            .put("double", DataType.DOUBLE)
            .put("ip", DataType.IP)
            .put("object", DataType.OBJECT)
            .put("nested", DataType.OBJECT).build();

    public DocIndexMetaData(IndexMetaData metaData, TableIdent ident) throws IOException {
        this.ident = ident;
        this.metaData = metaData;
        this.isAlias = !metaData.getIndex().equals(ident.name());
        this.numberOfShards = metaData.numberOfShards();
        Settings settings = metaData.getSettings();
        String autoExpandReplicas = settings.get(IndexMetaData.SETTING_AUTO_EXPAND_REPLICAS);
        if (autoExpandReplicas != null && !Booleans.isExplicitFalse(autoExpandReplicas)) {
            this.numberOfReplicas = new BytesRef(autoExpandReplicas);
        } else {
            this.numberOfReplicas = new BytesRef(settings.get(IndexMetaData.SETTING_NUMBER_OF_REPLICAS));
        }
        this.aliases = ImmutableSet.copyOf(metaData.aliases().keys().toArray(String.class));
        this.defaultMappingMetaData = this.metaData.mappingOrDefault(Constants.DEFAULT_MAPPING_TYPE);
        if (defaultMappingMetaData == null) {
            this.defaultMappingMap = new HashMap<>();
        } else {
            this.defaultMappingMap = this.defaultMappingMetaData.sourceAsMap();
        }

        prepareCrateMeta();
    }

    @SuppressWarnings("unchecked")
    private void prepareCrateMeta() {
        Object meta = defaultMappingMap.get("_meta");
        if (meta != null) {
            assert meta instanceof Map;
            metaMap = (Map<String, Object>)meta;
            Object indices = metaMap.get("indices");
            if (indices != null) {
                assert indices instanceof Map;
                indicesMap = (Map<String, Object>)indices;
            } else {
                indicesMap = new HashMap<>();
            }

            Object columns = metaMap.get("columns");
            if (columns != null) {
                assert columns instanceof Map;
                metaColumnsMap = (Map<String, Object>)columns;
            } else {
                metaColumnsMap = new HashMap<>();
            }
            Object partitionedBy = metaMap.get("partitioned_by");
            if (partitionedBy != null) {
                assert partitionedBy instanceof List;
                partitionedByList = (List<List<String>>)partitionedBy;
            } else {
                partitionedByList = ImmutableList.of();
            }
        } else {
            metaMap = new HashMap<>();
            indicesMap = new HashMap<>();
            metaColumnsMap = new HashMap<>();
            partitionedByList = ImmutableList.of();
        }
    }

    private void add(ColumnIdent column, DataType type) {
        add(column, type, ReferenceInfo.ObjectType.DYNAMIC, false);
    }

    private void addPartitioned(ColumnIdent column, DataType type) {
        add(column, type, ReferenceInfo.ObjectType.DYNAMIC, true);
    }

    private void add(ColumnIdent column, DataType type, ReferenceInfo.ObjectType objectType,
                     boolean partitioned) {
        // don't include indices in the column references
        if (indicesMap.keySet().contains(column.name())) {
            return;
        }

        ReferenceInfo info = newInfo(column, type, objectType);
        if (info.ident().isColumn()) {
            columnsBuilder.add(info);
        }
        referencesBuilder.put(info.ident().columnIdent(), info);
        if (partitioned) {
            partitionedByColumnsBuilder.add(info);
        }
    }

    private ReferenceInfo newInfo(ColumnIdent column, DataType type, ReferenceInfo.ObjectType objectType) {
        RowGranularity granularity = RowGranularity.DOC;
        if (partitionedBy.contains(column.name())) {
            granularity = RowGranularity.INDEX;
        }
        return new ReferenceInfo(new ReferenceIdent(ident, column), granularity, type, objectType);
    }

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    private DataType getColumnDataType(String columnName,
                                       @Nullable ColumnIdent columnIdent,
                                       Map<String, Object> columnProperties) {
        DataType type;
        String typeName = (String) columnProperties.get("type");

        if (typeName == null) {
            if (columnProperties.containsKey("properties")) {
                type = DataType.OBJECT;
            } else {
                return DataType.NOT_SUPPORTED;
            }
        } else {
            typeName = typeName.toLowerCase(Locale.ENGLISH);
            type = Objects.firstNonNull(dataTypeMap.get(typeName), DataType.NOT_SUPPORTED);
        }

        Optional<String> collectionType = getCollectionType(columnName, columnIdent);
        if (collectionType.isPresent() && collectionType.get().equals("array")) {
            DataType arrayType = DataType.ARRAY_TYPES_MAP.get(type);
            type = Objects.firstNonNull(arrayType, type);
        }

        return type;
    }

    /**
     * use to get the collection type (array | set | null) from the "_meta" columns property.
     *
     * @param columnName the name of the column that is being looked at.
     * @param columnIdent the "previous" parts of the column if it is a nested column
     */
    private Optional<String> getCollectionType(String columnName, @Nullable ColumnIdent columnIdent) {
        if (columnIdent == null) {
            Object o = metaColumnsMap.get(columnName);
            if (o == null) {
                return Optional.absent();
            }
            assert o instanceof Map;
            Map metaColumn = (Map)o;
            Object collection_type = metaColumn.get("collection_type");
            if (collection_type != null) {
                return Optional.of(collection_type.toString());
            }
            return Optional.absent();
        }

        List<String> path = new ArrayList<>(columnIdent.path());
        path.add(0, columnName);
        path.add(0, columnIdent.name());
        return getCollectionTypeFromNested(path);
    }

    private Optional<String> getCollectionTypeFromNested(List<String> path) {
        Map metaColumn = metaColumnsMap;
        while (path.size() > 1) {
            Object o = metaColumn.get(path.get(0));
            if (o == null) {
                break;
            }
            assert o instanceof Map;
            metaColumn = (Map) ((Map) o).get("properties");
            path = path.subList(1, path.size());
        }
        if (metaColumn == null) {
            return Optional.absent();
        }

        Object o = metaColumn.get(path.get(0));
        if (o == null) {
            return Optional.absent();
        }
        assert o instanceof Map;
        Object collection_type = ((Map)o).get("collection_type");
        if (collection_type != null) {
            return Optional.of(collection_type.toString());
        }
        return Optional.absent();
    }

    private ColumnIdent childIdent(ColumnIdent ident, String name) {
        if (ident == null) {
            return new ColumnIdent(name);
        }
        if (ident.isColumn()) {
            return new ColumnIdent(ident.name(), name);
        } else {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            for (String s : ident.path()) {
                builder.add(s);
            }
            builder.add(name);
            return new ColumnIdent(ident.name(), builder.build());
        }
    }

    @SuppressWarnings("unchecked")
    private void internalExtractColumnDefinitions(ColumnIdent columnIdent,
                                                  Map<String, Object> propertiesMap) {
        if (propertiesMap == null) {
            return;
        }

        for (Map.Entry<String, Object> columnEntry : propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map) columnEntry.getValue();
            DataType columnDataType = getColumnDataType(columnEntry.getKey(), columnIdent, columnProperties);

            if (columnDataType == DataType.OBJECT || columnDataType == DataType.OBJECT_ARRAY) {
                ReferenceInfo.ObjectType objectType =
                        ReferenceInfo.ObjectType.of(columnProperties.get("dynamic"));
                ColumnIdent newIdent = childIdent(columnIdent, columnEntry.getKey());
                add(newIdent, columnDataType, objectType, false);

                if (columnProperties.get("properties") != null) {
                    // walk nested
                    internalExtractColumnDefinitions(newIdent, (Map<String, Object>) columnProperties.get("properties"));
                }
            } else if (columnDataType != DataType.NOT_SUPPORTED) {
                ColumnIdent newIdent = childIdent(columnIdent, columnEntry.getKey());
                add(newIdent, columnDataType);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private ImmutableList<String> getPrimaryKey() {
        Map<String, Object> metaMap = (Map<String, Object>) defaultMappingMap.get("_meta");
        if (metaMap != null) {
            ImmutableList.Builder<String> builder = ImmutableList.builder();
            Object pKeys = metaMap.get("primary_keys");
            if (pKeys instanceof String) {
                builder.add((String) pKeys);
            } else if (pKeys instanceof Collection) {
                Collection keys = (Collection)pKeys;
                if (keys.isEmpty()) {
                    hasAutoGeneratedPrimaryKey = true;
                    return ImmutableList.of("_id");
                }
                builder.addAll(keys);
            }
            return builder.build();
        }
        hasAutoGeneratedPrimaryKey = true;
        return ImmutableList.of("_id");
    }

    @SuppressWarnings("unchecked")
    private ImmutableList<String> getPartitionedBy() {
        Map<String, Object> metaMap = (Map<String, Object>) defaultMappingMap.get("_meta");
        if (metaMap != null) {
            List<List<String>> pKeys = (List)metaMap.get("partitioned_by");
            if (pKeys != null) {
                ImmutableList.Builder<String> builder = ImmutableList.builder();
                for (List<String> partitionedByInfo : pKeys) {
                    builder.add(partitionedByInfo.get(0));
                }
                return builder.build();
            }
        }
        return ImmutableList.of();
    }

    @SuppressWarnings("unchecked")
    private void createColumnDefinitions() {
        Map<String, Object> propertiesMap = (Map<String, Object>) defaultMappingMap.get("properties");
        internalExtractColumnDefinitions(null, propertiesMap);
        extractPartitionedByColumns();
    }

    private void extractPartitionedByColumns() {
        for (List<String> partitioned : partitionedByList) {
            ColumnIdent ident;
            List<String> path = Splitter.on('.').splitToList(partitioned.get(0));
            if (path.size() > 1) {
                ident = new ColumnIdent(path.get(0), path.subList(1, path.size()));
            } else {
                ident = new ColumnIdent(path.get(0));
            }
            DataType type = getColumnDataType(path.get(0),
                    ident,
                    new MapBuilder<String, Object>().put("type", partitioned.get(1)).map());
            addPartitioned(ident, type);
        }
    }

    private String getRoutingCol() {
        if (defaultMappingMetaData != null) {
            Map<String, Object> metaMap = (Map<String, Object>) defaultMappingMap.get("_meta");
            if (metaMap != null) {
                String routingPath = (String)metaMap.get("routing");
                if (routingPath != null) {
                    return routingPath;
                }
            }
        }

        if (primaryKey.size() == 1) {
            return primaryKey.get(0);
        }
        return ID;
    }

    public DocIndexMetaData build() {
        partitionedBy = getPartitionedBy();
        createColumnDefinitions();
        columns = columnsBuilder.build();
        partitionedByColumns = partitionedByColumnsBuilder.build();

        for (Tuple<ColumnIdent, ReferenceInfo> sysColumns : DocSysColumns.forTable(ident)) {
            referencesBuilder.put(sysColumns.v1(), sysColumns.v2());
        }

        references = referencesBuilder.build();
        primaryKey = getPrimaryKey();
        routingCol = getRoutingCol();
        return this;
    }

    public ImmutableMap<ColumnIdent, ReferenceInfo> references() {
        return references;
    }

    public ImmutableList<ReferenceInfo> columns() {
        return columns;
    }

    public ImmutableList<ReferenceInfo> partitionedByColumns() {
        return partitionedByColumns;
    }

    public ImmutableList<String> primaryKey() {
        return primaryKey;
    }

    public String routingCol() {
        return routingCol;
    }

    public boolean schemaEquals(DocIndexMetaData other) {
        if (this == other) return true;
        if (other == null) return false;

        // TODO: when analyzers are exposed in the info, equality has to be checked on them
        // see: TransportSQLActionTest.testSelectTableAliasSchemaExceptionColumnDefinition
        if (columns != null ? !columns.equals(other.columns) : other.columns != null) return false;
        if (primaryKey != null ? !primaryKey.equals(other.primaryKey) : other.primaryKey != null) return false;
        if (references != null ? !references.equals(other.references) : other.references != null) return false;
        if (!routingCol.equals(other.routingCol)) return false;

        return true;
    }

    public DocIndexMetaData merge(DocIndexMetaData other) {
        // TODO: merge schemas if not equal, for now we just return this after making sure the schema is the same
        if (schemaEquals(other)) {
            return this;
        } else {
            throw new TableAliasSchemaException(other.name());
        }
    }

    private String name() {
        return ident.name();
    }

    /**
     * @return the name of the underlying index even if this table is referenced by alias
     */
    public String concreteIndexName() {
        return metaData.index();
    }

    public boolean isAlias() {
        return isAlias;
    }

    public Set<String> aliases() {
        return aliases;
    }

    public boolean hasAutoGeneratedPrimaryKey() {
        return hasAutoGeneratedPrimaryKey;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public BytesRef numberOfReplicas() {
        return numberOfReplicas;
    }

    public ImmutableList<String> partitionedBy() {
        return partitionedBy;
    }
}
