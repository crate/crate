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
import com.google.common.collect.*;
import io.crate.Constants;
import io.crate.PartitionName;
import io.crate.exceptions.TableAliasSchemaException;
import io.crate.metadata.*;
import io.crate.planner.RowGranularity;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.TransportPutIndexTemplateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class DocIndexMetaData {

    private static final String ID = "_id";
    private static final ColumnIdent ID_IDENT = new ColumnIdent(ID);
    private final IndexMetaData metaData;


    private final MappingMetaData defaultMappingMetaData;
    private final Map<String, Object> defaultMappingMap;

    private final Map<ColumnIdent, IndexReferenceInfo.Builder> indicesBuilder = new HashMap<>();

    private final ImmutableSortedSet.Builder<ReferenceInfo> columnsBuilder = ImmutableSortedSet.orderedBy(new Comparator<ReferenceInfo>() {
        @Override
        public int compare(ReferenceInfo o1, ReferenceInfo o2) {
            return o1.ident().columnIdent().fqn().compareTo(o2.ident().columnIdent().fqn());
        }
    });

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
    private ImmutableMap<ColumnIdent, IndexReferenceInfo> indices;
    private ImmutableList<ReferenceInfo> partitionedByColumns;
    private ImmutableMap<ColumnIdent, ReferenceInfo> references;
    private ImmutableList<ColumnIdent> primaryKey;
    private ColumnIdent routingCol;
    private ImmutableList<ColumnIdent> partitionedBy;
    private final boolean isAlias;
    private final Set<String> aliases;
    private boolean hasAutoGeneratedPrimaryKey = false;

    private final static ImmutableMap<String, DataType> dataTypeMap = ImmutableMap.<String, DataType>builder()
            .put("date", DataTypes.TIMESTAMP)
            .put("string", DataTypes.STRING)
            .put("boolean", DataTypes.BOOLEAN)
            .put("byte", DataTypes.BYTE)
            .put("short", DataTypes.SHORT)
            .put("integer", DataTypes.INTEGER)
            .put("long", DataTypes.LONG)
            .put("float", DataTypes.FLOAT)
            .put("double", DataTypes.DOUBLE)
            .put("ip", DataTypes.IP)
            .put("geo_point", DataTypes.GEO_POINT)
            .put("object", DataTypes.OBJECT)
            .put("nested", DataTypes.OBJECT).build();

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
    private static <T> T getNested(Map map, String key) {
        return (T)map.get(key);
    }


    private void prepareCrateMeta() {
        metaMap = getNested(defaultMappingMap, "_meta");
        if (metaMap != null) {
            indicesMap = getNested(metaMap, "indices");
            if (indicesMap == null) {
                indicesMap = ImmutableMap.of();
            }
            metaColumnsMap = getNested(metaMap, "columns");
            if (metaColumnsMap == null) {
                metaColumnsMap = ImmutableMap.of();
            }

            partitionedByList = getNested(metaMap, "partitioned_by");
            if (partitionedByList == null) {
                partitionedByList = ImmutableList.of();
            }
        } else {
            metaMap = new HashMap<>();
            indicesMap = new HashMap<>();
            metaColumnsMap = new HashMap<>();
            partitionedByList = ImmutableList.of();
        }
    }

    private void addPartitioned(ColumnIdent column, DataType type) {
        add(column, type, ReferenceInfo.ObjectType.DYNAMIC, ReferenceInfo.IndexType.NOT_ANALYZED, true);
    }

    private void add(ColumnIdent column, DataType type, ReferenceInfo.IndexType indexType) {
        add(column, type, ReferenceInfo.ObjectType.DYNAMIC, indexType, false);
    }

    private void add(ColumnIdent column, DataType type, ReferenceInfo.ObjectType objectType,
                     ReferenceInfo.IndexType indexType, boolean partitioned) {
        ReferenceInfo info = newInfo(column, type, objectType, indexType);
        if (info.ident().isColumn()) {
            columnsBuilder.add(info);
        }
        referencesBuilder.put(info.ident().columnIdent(), info);
        if (partitioned) {
            partitionedByColumnsBuilder.add(info);
        }
    }

    private ReferenceInfo newInfo(ColumnIdent column,
                                  DataType type,
                                  ReferenceInfo.ObjectType objectType,
                                  ReferenceInfo.IndexType indexType) {
        RowGranularity granularity = RowGranularity.DOC;
        if (partitionedBy.contains(column)) {
            granularity = RowGranularity.PARTITION;
        }
        return new ReferenceInfo(new ReferenceIdent(ident, column), granularity, type,
                objectType, indexType);
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
                type = DataTypes.OBJECT;
            } else {
                return DataTypes.NOT_SUPPORTED;
            }
        } else {
            typeName = typeName.toLowerCase(Locale.ENGLISH);
            type = Objects.firstNonNull(dataTypeMap.get(typeName), DataTypes.NOT_SUPPORTED);
        }

        Optional<String> collectionType = getCollectionType(columnName, columnIdent);
        if (collectionType.isPresent() && collectionType.get().equals("array")) {
            type = new ArrayType(type);
        }
        return type;
    }

    private ReferenceInfo.IndexType getColumnIndexType(String columnName,
                                                       Map<String, Object> columnProperties) {
        String indexType = (String) columnProperties.get("index");
        String analyzerName = (String) columnProperties.get("analyzer");
        if (indexType != null) {
            if (indexType.equals(ReferenceInfo.IndexType.NOT_ANALYZED.toString())) {
                return ReferenceInfo.IndexType.NOT_ANALYZED;
            } else if (indexType.equals(ReferenceInfo.IndexType.NO.toString())) {
                return ReferenceInfo.IndexType.NO;
            } else if (indexType.equals(ReferenceInfo.IndexType.ANALYZED.toString())
                    && analyzerName != null && !analyzerName.equals("keyword")) {
                return ReferenceInfo.IndexType.ANALYZED;
            }
        } // default indexType is analyzed so need to check analyzerName if indexType is null
        else if (analyzerName != null && !analyzerName.equals("keyword")) {
            return ReferenceInfo.IndexType.ANALYZED;
        }
        return ReferenceInfo.IndexType.NOT_ANALYZED;
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

    /**
     * extracts index definitions as well
     */
    @SuppressWarnings("unchecked")
    private void internalExtractColumnDefinitions(ColumnIdent columnIdent,
                                                  Map<String, Object> propertiesMap) {
        if (propertiesMap == null) {
            return;
        }

        for (Map.Entry<String, Object> columnEntry : propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map) columnEntry.getValue();
            DataType columnDataType = getColumnDataType(columnEntry.getKey(), columnIdent, columnProperties);
            ReferenceInfo.IndexType columnIndexType = getColumnIndexType(columnEntry.getKey(), columnProperties);
            List<String> copyToColumns = getNested(columnProperties, "copy_to");

            if (columnDataType == DataTypes.OBJECT
                    || ( columnDataType.id() == ArrayType.ID
                        && ((ArrayType)columnDataType).innerType() == DataTypes.OBJECT )) {
                ReferenceInfo.ObjectType objectType =
                        ReferenceInfo.ObjectType.of(columnProperties.get("dynamic"));
                ColumnIdent newIdent = childIdent(columnIdent, columnEntry.getKey());
                add(newIdent, columnDataType, objectType, ReferenceInfo.IndexType.NO, false);

                if (columnProperties.get("properties") != null) {
                    // walk nested
                    internalExtractColumnDefinitions(newIdent, (Map<String, Object>) columnProperties.get("properties"));
                }
            } else if (columnDataType != DataTypes.NOT_SUPPORTED) {
                ColumnIdent newIdent = childIdent(columnIdent, columnEntry.getKey());

                // extract columns this column is copied to, needed for indices
                if (copyToColumns != null) {
                    for (String copyToColumn : copyToColumns) {
                        ColumnIdent targetIdent = ColumnIdent.fromPath(copyToColumn);
                        IndexReferenceInfo.Builder builder = getOrCreateIndexBuilder(targetIdent);
                        builder.addColumn(newInfo(newIdent, columnDataType, ReferenceInfo.ObjectType.DYNAMIC, columnIndexType));
                    }
                }
                // is it an index?
                if (indicesMap.containsKey(newIdent.fqn())) {
                    String analyzer = getNested(columnProperties, "analyzer");
                    IndexReferenceInfo.Builder builder = getOrCreateIndexBuilder(newIdent);
                    builder.analyzer(analyzer)
                           .indexType(columnIndexType)
                           .ident(new ReferenceIdent(ident, newIdent));
                } else {
                    add(newIdent, columnDataType, columnIndexType);
                }
            }
        }
    }

    private IndexReferenceInfo.Builder getOrCreateIndexBuilder(ColumnIdent ident) {
        IndexReferenceInfo.Builder builder = indicesBuilder.get(ident);
        if (builder == null) {
            builder = new IndexReferenceInfo.Builder();
            indicesBuilder.put(ident, builder);
        }
        return builder;
    }

    private ImmutableList<ColumnIdent> getPrimaryKey() {
        Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
        if (metaMap == null) {
            hasAutoGeneratedPrimaryKey = true;
            return ImmutableList.of(ID_IDENT);
        }

        ImmutableList.Builder<ColumnIdent> builder = ImmutableList.builder();
        Object pKeys = metaMap.get("primary_keys");
        if (pKeys == null) {
            hasAutoGeneratedPrimaryKey = true;
            return ImmutableList.of(ID_IDENT);
        }

        if (pKeys instanceof String) {
            builder.add(ColumnIdent.fromPath((String) pKeys));
        } else if (pKeys instanceof Collection) {
            Collection keys = (Collection)pKeys;
            if (keys.isEmpty()) {
                hasAutoGeneratedPrimaryKey = true;
                return ImmutableList.of(ID_IDENT);
            }
            for (Object pkey : keys) {
                builder.add(ColumnIdent.fromPath(pkey.toString()));
            }
        }
        return builder.build();
    }

    private ImmutableList<ColumnIdent> getPartitionedBy() {
        ImmutableList.Builder<ColumnIdent> builder = ImmutableList.builder();
        for (List<String> partitionedByInfo : partitionedByList) {
            builder.add(ColumnIdent.fromPath(partitionedByInfo.get(0)));
        }
        return builder.build();
    }

    private void createColumnDefinitions() {
        Map<String, Object> propertiesMap = getNested(defaultMappingMap, "properties");
        internalExtractColumnDefinitions(null, propertiesMap);
        extractPartitionedByColumns();
    }

    private ImmutableMap<ColumnIdent, IndexReferenceInfo> createIndexDefinitions() {
        ImmutableMap.Builder<ColumnIdent, IndexReferenceInfo> builder = ImmutableMap.builder();
        for (Map.Entry<ColumnIdent, IndexReferenceInfo.Builder> entry: indicesBuilder.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().build());
        }
        indices = builder.build();
        return indices;
    }

    private void extractPartitionedByColumns() {
        for (List<String> partitioned : partitionedByList) {
            ColumnIdent ident = ColumnIdent.fromPath(partitioned.get(0));
            DataType type = getColumnDataType(ident.fqn(),
                    ident,
                    new MapBuilder<String, Object>().put("type", partitioned.get(1)).map());
            addPartitioned(ident, type);
        }
    }

    private ColumnIdent getRoutingCol() {
        if (defaultMappingMetaData != null) {
            Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
            if (metaMap != null) {
                String routingPath = (String)metaMap.get("routing");
                if (routingPath != null) {
                    return ColumnIdent.fromPath(routingPath);
                }
            }
        }
        if (primaryKey.size() == 1) {
            return primaryKey.get(0);
        }
        return ID_IDENT;
    }

    public DocIndexMetaData build() {
        partitionedBy = getPartitionedBy();
        createColumnDefinitions();
        indices = createIndexDefinitions();
        columns = ImmutableList.copyOf(columnsBuilder.build());
        partitionedByColumns = partitionedByColumnsBuilder.build();

        for (Tuple<ColumnIdent, ReferenceInfo> sysColumn : DocSysColumns.forTable(ident)) {
            referencesBuilder.put(sysColumn.v1(), sysColumn.v2());
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

    public ImmutableMap<ColumnIdent, IndexReferenceInfo> indices() {
        return indices;
    }

    public ImmutableList<ReferenceInfo> partitionedByColumns() {
        return partitionedByColumns;
    }

    public ImmutableList<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    public ColumnIdent routingCol() {
        return routingCol;
    }

    public boolean schemaEquals(DocIndexMetaData other) {
        if (this == other) return true;
        if (other == null) return false;

        // TODO: when analyzers are exposed in the info, equality has to be checked on them
        // see: TransportSQLActionTest.testSelectTableAliasSchemaExceptionColumnDefinition
        if (columns    != null ? !columns.equals(other.columns)       : other.columns    != null) return false;
        if (primaryKey != null ? !primaryKey.equals(other.primaryKey) : other.primaryKey != null) return false;
        if (indices    != null ? !indices.equals(other.indices)       : other.indices    != null) return false;
        if (references != null ? !references.equals(other.references) : other.references != null) return false;
        if (routingCol != null ? !routingCol.equals(other.routingCol) : other.routingCol != null) return false;

        return true;
    }

    public DocIndexMetaData merge(DocIndexMetaData other,
                                  TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                                  boolean thisIsCreatedFromTemplate)
            throws IOException {
        if (schemaEquals(other)) {
            return this;
        } else if (thisIsCreatedFromTemplate) {
            if (this.references.size() < other.references.size()) {
                // this is older, update template and return other
                updateTemplate(other, transportPutIndexTemplateAction);
                return other;
            } else if (references().size() == other.references().size() &&
                    !references().keySet().equals(other.references().keySet())) {
                XContentHelper.update(defaultMappingMap, other.defaultMappingMap);
                updateTemplate(this, transportPutIndexTemplateAction);
                return this;
            }
            // other is older, just return this
            return this;
        } else {
            throw new TableAliasSchemaException(other.name());
        }
    }

    private void updateTemplate(DocIndexMetaData md,
                                TransportPutIndexTemplateAction transportPutIndexTemplateAction) {
        String templateName = PartitionName.templateName(name());
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
                .mapping(Constants.DEFAULT_MAPPING_TYPE, md.defaultMappingMap)
                .create(false)
                .settings(md.metaData.settings())
                .template(templateName + "*");
        for (String alias : md.aliases()) {
            request = request.alias(new Alias(alias));
        }
        transportPutIndexTemplateAction.execute(request);
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

    public ImmutableList<ColumnIdent> partitionedBy() {
        return partitionedBy;
    }
}
