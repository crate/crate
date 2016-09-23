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

import com.google.common.base.MoreObjects;
import com.google.common.collect.*;
import io.crate.Constants;
import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.ParameterContext;
import io.crate.analyze.TableParameterInfo;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.exceptions.TableAliasSchemaException;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.Operation;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Expression;
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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;

public class DocIndexMetaData {

    private static final String ID = "_id";
    private final IndexMetaData metaData;

    private final MappingMetaData defaultMappingMetaData;
    private final Map<String, Object> defaultMappingMap;

    private final Map<ColumnIdent, IndexReference.Builder> indicesBuilder = new HashMap<>();

    private final ImmutableSortedSet.Builder<Reference> columnsBuilder = ImmutableSortedSet.orderedBy(new Comparator<Reference>() {
        @Override
        public int compare(Reference o1, Reference o2) {
            return o1.ident().columnIdent().fqn().compareTo(o2.ident().columnIdent().fqn());
        }
    });

    // columns should be ordered
    private final ImmutableMap.Builder<ColumnIdent, Reference> referencesBuilder = ImmutableSortedMap.naturalOrder();
    private final ImmutableList.Builder<Reference> partitionedByColumnsBuilder = ImmutableList.builder();
    private final ImmutableList.Builder<GeneratedReference> generatedColumnReferencesBuilder = ImmutableList.builder();

    private final Functions functions;
    private final TableIdent ident;
    private final int numberOfShards;
    private final BytesRef numberOfReplicas;
    private final ImmutableMap<String, Object> tableParameters;
    private final Map<String, Object> indicesMap;
    private final List<List<String>> partitionedByList;
    private final Set<Operation> supportedOperations;
    private final boolean isAlias;
    private final Set<String> aliases;
    private ImmutableList<Reference> columns;
    private ImmutableMap<ColumnIdent, IndexReference> indices;
    private ImmutableList<Reference> partitionedByColumns;
    private ImmutableList<GeneratedReference> generatedColumnReferences;
    private ImmutableMap<ColumnIdent, Reference> references;
    private ImmutableList<ColumnIdent> primaryKey;
    private ImmutableCollection<ColumnIdent> notNullColumns;
    private ColumnIdent routingCol;
    private ImmutableList<ColumnIdent> partitionedBy;
    private boolean hasAutoGeneratedPrimaryKey = false;

    private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
    private Map<String, String> generatedColumns;

    public DocIndexMetaData(Functions functions, IndexMetaData metaData, TableIdent ident) throws IOException {
        this.functions = functions;
        this.ident = ident;
        this.metaData = metaData;
        this.isAlias = !metaData.getIndex().equals(ident.indexName());
        this.numberOfShards = metaData.getNumberOfShards();
        Settings settings = metaData.getSettings();
        this.numberOfReplicas = NumberOfReplicas.fromSettings(settings);
        this.aliases = ImmutableSet.copyOf(metaData.getAliases().keys().toArray(String.class));
        this.defaultMappingMetaData = this.metaData.mappingOrDefault(Constants.DEFAULT_MAPPING_TYPE);
        if (defaultMappingMetaData == null) {
            this.defaultMappingMap = ImmutableMap.of();
        } else {
            this.defaultMappingMap = this.defaultMappingMetaData.sourceAsMap();
        }
        this.tableParameters = TableParameterInfo.tableParametersFromIndexMetaData(metaData);

        Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
        indicesMap = getNested(metaMap, "indices", ImmutableMap.<String, Object>of());
        partitionedByList = getNested(metaMap, "partitioned_by", ImmutableList.<List<String>>of());
        generatedColumns = getNested(metaMap, "generated_columns", ImmutableMap.<String, String>of());
        if (isAlias && partitionedByList.isEmpty()) {
            supportedOperations = Operation.READ_ONLY;
        } else {
            supportedOperations = Operation.buildFromIndexSettings(metaData.getSettings());
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T getNested(Map map, String key) {
        return (T) map.get(key);
    }

    private static <T> T getNested(@Nullable Map map, String key, T defaultValue) {
        if (map == null) {
            return defaultValue;
        }
        Object o = map.get(key);
        if (o == null) {
            return defaultValue;
        }
        //noinspection unchecked
        return (T) o;
    }

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    public static DataType getColumnDataType(Map<String, Object> columnProperties) {
        DataType type;
        String typeName = (String) columnProperties.get("type");

        if (typeName == null) {
            if (columnProperties.containsKey("properties")) {
                type = DataTypes.OBJECT;
            } else {
                return DataTypes.NOT_SUPPORTED;
            }
        } else if (typeName.equalsIgnoreCase("array")) {

            Map<String, Object> innerProperties = getNested(columnProperties, "inner");
            DataType innerType = getColumnDataType(innerProperties);
            type = new ArrayType(innerType);
        } else {
            typeName = typeName.toLowerCase(Locale.ENGLISH);
            type = MoreObjects.firstNonNull(DataTypes.ofMappingName(typeName), DataTypes.NOT_SUPPORTED);
        }
        return type;
    }

    private static ColumnIdent childIdent(@Nullable ColumnIdent ident, String name) {
        if (ident == null) {
            return new ColumnIdent(name);
        }
        return ColumnIdent.getChild(ident, name);
    }

    private void addPartitioned(ColumnIdent column, DataType type) {
        add(column, type, ColumnPolicy.DYNAMIC, Reference.IndexType.NOT_ANALYZED, true, true);
    }

    private void add(ColumnIdent column, DataType type, Reference.IndexType indexType, boolean isNotNull) {
        add(column, type, ColumnPolicy.DYNAMIC, indexType, false, isNotNull);
    }

    private void add(ColumnIdent column,
                     DataType type,
                     ColumnPolicy columnPolicy,
                     Reference.IndexType indexType,
                     boolean partitioned,
                     boolean isNotNull) {
        Reference info;
        String generatedExpression = generatedColumns.get(column.fqn());
        if (generatedExpression == null) {
            info = newInfo(column, type, columnPolicy, indexType, isNotNull);
        } else {
            info = newGeneratedColumnInfo(column, type, columnPolicy, indexType, generatedExpression, isNotNull);
        }

        // don't add it if there is a partitioned equivalent of this column
        if (partitioned || !(partitionedBy != null && partitionedBy.contains(column))) {
            if (info.ident().isColumn()) {
                columnsBuilder.add(info);
            }
            referencesBuilder.put(info.ident().columnIdent(), info);
            if (info instanceof GeneratedReference) {
                generatedColumnReferencesBuilder.add((GeneratedReference) info);
            }
        }
        if (partitioned) {
            partitionedByColumnsBuilder.add(info);
        }
    }

    private void addGeoReference(ColumnIdent column,
                                 @Nullable String tree,
                                 @Nullable String precision,
                                 @Nullable Integer treeLevels,
                                 @Nullable Double distanceErrorPct) {
        GeoReference info = new GeoReference(
            refIdent(column),
            tree,
            precision,
            treeLevels,
            distanceErrorPct);
        columnsBuilder.add(info);
        referencesBuilder.put(column, info);
    }

    private ReferenceIdent refIdent(ColumnIdent column) {
        return new ReferenceIdent(ident, column);
    }

    private GeneratedReference newGeneratedColumnInfo(ColumnIdent column,
                                                      DataType type,
                                                      ColumnPolicy columnPolicy,
                                                      Reference.IndexType indexType,
                                                      String generatedExpression,
                                                      boolean isNotNull) {
        return new GeneratedReference(
            refIdent(column), granularity(column), type, columnPolicy, indexType, generatedExpression, isNotNull);
    }

    private RowGranularity granularity(ColumnIdent column) {
        if (partitionedBy.contains(column)) {
            return RowGranularity.PARTITION;
        }
        return RowGranularity.DOC;
    }

    private Reference newInfo(ColumnIdent column,
                              DataType type,
                              ColumnPolicy columnPolicy,
                              Reference.IndexType indexType,
                              boolean nullable) {
        return new Reference(refIdent(column), granularity(column), type, columnPolicy, indexType, nullable);
    }

    private Reference.IndexType getColumnIndexType(Map<String, Object> columnProperties) {
        String indexType = (String) columnProperties.get("index");
        String analyzerName = (String) columnProperties.get("analyzer");
        if (indexType != null) {
            if (indexType.equals(Reference.IndexType.NOT_ANALYZED.toString())) {
                return Reference.IndexType.NOT_ANALYZED;
            } else if (indexType.equals(Reference.IndexType.NO.toString())) {
                return Reference.IndexType.NO;
            } else if (indexType.equals(Reference.IndexType.ANALYZED.toString())
                       && analyzerName != null && !analyzerName.equals("keyword")) {
                return Reference.IndexType.ANALYZED;
            }
        } // default indexType is analyzed so need to check analyzerName if indexType is null
        else if (analyzerName != null && !analyzerName.equals("keyword")) {
            return Reference.IndexType.ANALYZED;
        }
        return Reference.IndexType.NOT_ANALYZED;
    }

    /**
     * extracts index definitions as well
     */
    @SuppressWarnings("unchecked")
    private void internalExtractColumnDefinitions(@Nullable ColumnIdent columnIdent,
                                                  @Nullable Map<String, Object> propertiesMap) {
        if (propertiesMap == null) {
            return;
        }
        for (Map.Entry<String, Object> columnEntry : propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map) columnEntry.getValue();
            DataType columnDataType = getColumnDataType(columnProperties);
            ColumnIdent newIdent = childIdent(columnIdent, columnEntry.getKey());

            boolean nullable = !notNullColumns.contains(newIdent);
            columnProperties = furtherColumnProperties(columnProperties);
            Reference.IndexType columnIndexType = getColumnIndexType(columnProperties);
            if (columnDataType == DataTypes.GEO_SHAPE) {
                String geoTree = (String) columnProperties.get("tree");
                String precision = (String) columnProperties.get("precision");
                Integer treeLevels = (Integer) columnProperties.get("tree_levels");
                Double distanceErrorPct = (Double) columnProperties.get("distance_error_pct");
                addGeoReference(newIdent, geoTree, precision, treeLevels, distanceErrorPct);
            } else if (columnDataType == DataTypes.OBJECT
                       || (columnDataType.id() == ArrayType.ID
                           && ((ArrayType) columnDataType).innerType() == DataTypes.OBJECT)) {
                ColumnPolicy columnPolicy =
                    ColumnPolicy.of(columnProperties.get("dynamic"));
                add(newIdent, columnDataType, columnPolicy, Reference.IndexType.NO, false, nullable);

                if (columnProperties.get("properties") != null) {
                    // walk nested
                    internalExtractColumnDefinitions(newIdent, (Map<String, Object>) columnProperties.get("properties"));
                }
            } else if (columnDataType != DataTypes.NOT_SUPPORTED) {
                List<String> copyToColumns = getNested(columnProperties, "copy_to");

                // extract columns this column is copied to, needed for indices
                if (copyToColumns != null) {
                    for (String copyToColumn : copyToColumns) {
                        ColumnIdent targetIdent = ColumnIdent.fromPath(copyToColumn);
                        IndexReference.Builder builder = getOrCreateIndexBuilder(targetIdent);
                        builder.addColumn(newInfo(newIdent, columnDataType, ColumnPolicy.DYNAMIC, columnIndexType, false));
                    }
                }
                // is it an index?
                if (indicesMap.containsKey(newIdent.fqn())) {
                    IndexReference.Builder builder = getOrCreateIndexBuilder(newIdent);
                    builder.indexType(columnIndexType)
                        .analyzer((String) columnProperties.get("analyzer"));
                } else {
                    add(newIdent, columnDataType, columnIndexType, nullable);
                }
            }
        }
    }

    /**
     * get the real column properties from a possible array mapping,
     * keeping most of this stuff inside "inner"
     */
    private Map<String, Object> furtherColumnProperties(Map<String, Object> columnProperties) {
        if (columnProperties.get("inner") != null) {
            return (Map<String, Object>) columnProperties.get("inner");
        } else {
            return columnProperties;
        }
    }

    private IndexReference.Builder getOrCreateIndexBuilder(ColumnIdent ident) {
        IndexReference.Builder builder = indicesBuilder.get(ident);
        if (builder == null) {
            builder = new IndexReference.Builder(refIdent(ident));
            indicesBuilder.put(ident, builder);
        }
        return builder;
    }

    private ImmutableList<ColumnIdent> getPrimaryKey() {
        Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
        if (metaMap != null) {
            ImmutableList.Builder<ColumnIdent> builder = ImmutableList.builder();
            Object pKeys = metaMap.get("primary_keys");
            if (pKeys != null) {
                if (pKeys instanceof String) {
                    builder.add(ColumnIdent.fromPath((String) pKeys));
                    return builder.build();
                } else if (pKeys instanceof Collection) {
                    Collection keys = (Collection) pKeys;
                    if (!keys.isEmpty()) {
                        for (Object pkey : keys) {
                            builder.add(ColumnIdent.fromPath(pkey.toString()));
                        }
                        return builder.build();
                    }
                }
            }
        }
        if (getCustomRoutingCol() == null && partitionedByList.isEmpty()) {
            hasAutoGeneratedPrimaryKey = true;
            return ImmutableList.of(DocSysColumns.ID);
        }
        return ImmutableList.of();
    }

    private ImmutableCollection<ColumnIdent> getNotNullColumns() {
        Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
        if (metaMap != null) {
            ImmutableSet.Builder<ColumnIdent> builder = ImmutableSet.builder();
            Map<String, Object> constraintsMap = getNested(metaMap, "constraints");
            if (constraintsMap != null) {
                Object notNullColumnsMeta = constraintsMap.get("not_null");
                if (notNullColumnsMeta != null) {
                    Collection notNullColumns = (Collection) notNullColumnsMeta;
                    if (!notNullColumns.isEmpty()) {
                        for (Object notNullColumn : notNullColumns) {
                            builder.add(ColumnIdent.fromPath(notNullColumn.toString()));
                        }
                        return builder.build();
                    }
                }
            }
        }
        return ImmutableList.of();
    }

    private ImmutableList<ColumnIdent> getPartitionedBy() {
        ImmutableList.Builder<ColumnIdent> builder = ImmutableList.builder();
        for (List<String> partitionedByInfo : partitionedByList) {
            builder.add(ColumnIdent.fromPath(partitionedByInfo.get(0)));
        }
        return builder.build();
    }

    private ColumnPolicy getColumnPolicy() {
        Object dynamic = getNested(defaultMappingMap, "dynamic");
        if (ColumnPolicy.STRICT.value().equals(String.valueOf(dynamic).toLowerCase(Locale.ENGLISH))) {
            return ColumnPolicy.STRICT;
        } else if (Booleans.isExplicitFalse(String.valueOf(dynamic))) {
            return ColumnPolicy.IGNORED;
        } else {
            return ColumnPolicy.DYNAMIC;
        }
    }

    private void createColumnDefinitions() {
        Map<String, Object> propertiesMap = getNested(defaultMappingMap, "properties");
        internalExtractColumnDefinitions(null, propertiesMap);
        extractPartitionedByColumns();
    }

    private ImmutableMap<ColumnIdent, IndexReference> createIndexDefinitions() {
        ImmutableMap.Builder<ColumnIdent, IndexReference> builder = ImmutableMap.builder();
        for (Map.Entry<ColumnIdent, IndexReference.Builder> entry : indicesBuilder.entrySet()) {
            builder.put(entry.getKey(), entry.getValue().build());
        }
        indices = builder.build();
        return indices;
    }

    private void extractPartitionedByColumns() {
        for (Tuple<ColumnIdent, DataType> partitioned : PartitionedByMappingExtractor.extractPartitionedByColumns(partitionedByList)) {
            addPartitioned(partitioned.v1(), partitioned.v2());
        }
    }

    private ColumnIdent getCustomRoutingCol() {
        if (defaultMappingMetaData != null) {
            Map<String, Object> metaMap = getNested(defaultMappingMap, "_meta");
            if (metaMap != null) {
                String routingPath = (String) metaMap.get("routing");
                if (routingPath != null && !routingPath.equals(ID)) {
                    return ColumnIdent.fromPath(routingPath);
                }
            }
        }
        return null;
    }

    private ColumnIdent getRoutingCol() {
        ColumnIdent col = getCustomRoutingCol();
        if (col != null) {
            return col;
        }
        if (primaryKey.size() == 1) {
            return primaryKey.get(0);
        }
        return DocSysColumns.ID;
    }

    private void initializeGeneratedExpressions() {
        Collection<Reference> references = this.references.values();
        TableReferenceResolver tableReferenceResolver = new TableReferenceResolver(references);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, null, ParameterContext.EMPTY, tableReferenceResolver, null);
        ExpressionAnalysisContext context = new ExpressionAnalysisContext(new StmtCtx());
        for (Reference reference : generatedColumnReferences) {
            GeneratedReference generatedReference = (GeneratedReference) reference;
            Expression expression = SqlParser.createExpression(generatedReference.formattedGeneratedExpression());
            generatedReference.generatedExpression(expressionAnalyzer.convert(expression, context));
            generatedReference.referencedReferences(ImmutableList.copyOf(tableReferenceResolver.references()));
            tableReferenceResolver.references().clear();
        }
    }

    public DocIndexMetaData build() {
        notNullColumns = getNotNullColumns();
        partitionedBy = getPartitionedBy();
        columnPolicy = getColumnPolicy();
        createColumnDefinitions();
        indices = createIndexDefinitions();
        columns = ImmutableList.copyOf(columnsBuilder.build());
        partitionedByColumns = partitionedByColumnsBuilder.build();

        for (Tuple<ColumnIdent, Reference> sysColumn : DocSysColumns.forTable(ident)) {
            referencesBuilder.put(sysColumn.v1(), sysColumn.v2());
        }
        references = referencesBuilder.build();
        generatedColumnReferences = generatedColumnReferencesBuilder.build();
        primaryKey = getPrimaryKey();
        routingCol = getRoutingCol();

        initializeGeneratedExpressions();
        return this;
    }

    public ImmutableMap<ColumnIdent, Reference> references() {
        return references;
    }

    public ImmutableList<Reference> columns() {
        return columns;
    }

    public ImmutableMap<ColumnIdent, IndexReference> indices() {
        return indices;
    }

    public ImmutableList<Reference> partitionedByColumns() {
        return partitionedByColumns;
    }

    public ImmutableList<GeneratedReference> generatedColumnReferences() {
        return generatedColumnReferences;
    }

    public ImmutableList<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    public ColumnIdent routingCol() {
        return routingCol;
    }

    /**
     * Returns true if the schema of this and <code>other</code> is the same,
     * this includes the table name, as this is reflected in the ReferenceIdents of
     * the columns.
     */
    public boolean schemaEquals(DocIndexMetaData other) {
        if (this == other) return true;
        if (other == null) return false;

        // TODO: when analyzers are exposed in the info, equality has to be checked on them
        // see: TransportSQLActionTest.testSelectTableAliasSchemaExceptionColumnDefinition
        if (columns != null ? !columns.equals(other.columns) : other.columns != null) return false;
        if (primaryKey != null ? !primaryKey.equals(other.primaryKey) : other.primaryKey != null) return false;
        if (indices != null ? !indices.equals(other.indices) : other.indices != null) return false;
        if (references != null ? !references.equals(other.references) : other.references != null) return false;
        if (routingCol != null ? !routingCol.equals(other.routingCol) : other.routingCol != null) return false;

        return true;
    }

    protected DocIndexMetaData merge(DocIndexMetaData other,
                                     TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                                     boolean thisIsCreatedFromTemplate) throws IOException {
        if (schemaEquals(other)) {
            return this;
        } else if (thisIsCreatedFromTemplate) {
            if (this.references.size() < other.references.size()) {
                // this is older, update template and return other
                // settings in template are always authoritative for table information about
                // number_of_shards and number_of_replicas
                updateTemplate(other, transportPutIndexTemplateAction, this.metaData.getSettings());
                // merge the new mapping with the template settings
                return new DocIndexMetaData(
                    functions,
                    IndexMetaData.builder(other.metaData).settings(this.metaData.getSettings()).build(),
                    other.ident).build();
            } else if (references().size() == other.references().size() &&
                       !references().keySet().equals(other.references().keySet())) {
                XContentHelper.update(defaultMappingMap, other.defaultMappingMap, false);
                // update the template with new information
                updateTemplate(this, transportPutIndexTemplateAction, this.metaData.getSettings());
                return this;
            }
            // other is older, just return this
            return this;
        } else {
            throw new TableAliasSchemaException(other.ident.name());
        }
    }

    private void updateTemplate(DocIndexMetaData md,
                                TransportPutIndexTemplateAction transportPutIndexTemplateAction,
                                Settings updateSettings) {
        String templateName = PartitionName.templateName(ident.schema(), ident.name());
        PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
            .mapping(Constants.DEFAULT_MAPPING_TYPE, md.defaultMappingMap)
            .create(false)
            .settings(updateSettings)
            .template(templateName + "*");
        for (String alias : md.aliases()) {
            request = request.alias(new Alias(alias));
        }
        transportPutIndexTemplateAction.execute(request);
    }

    /**
     * @return the name of the underlying index even if this table is referenced by alias
     */
    public String concreteIndexName() {
        return metaData.getIndex();
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

    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    public ImmutableMap<String, Object> tableParameters() {
        return tableParameters;
    }

    private ImmutableMap<ColumnIdent, String> getAnalyzers(ColumnIdent columnIdent, Map<String, Object> propertiesMap) {
        ImmutableMap.Builder<ColumnIdent, String> builder = ImmutableMap.builder();
        for (Map.Entry<String, Object> columnEntry : propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map) columnEntry.getValue();
            DataType columnDataType = getColumnDataType(columnProperties);
            ColumnIdent newIdent = childIdent(columnIdent, columnEntry.getKey());
            columnProperties = furtherColumnProperties(columnProperties);
            if (columnDataType == DataTypes.OBJECT
                || (columnDataType.id() == ArrayType.ID
                    && ((ArrayType) columnDataType).innerType() == DataTypes.OBJECT)) {
                if (columnProperties.get("properties") != null) {
                    builder.putAll(getAnalyzers(newIdent, (Map<String, Object>) columnProperties.get("properties")));
                }
            }
            String analyzer = (String) columnProperties.get("analyzer");
            if (analyzer != null) {
                builder.put(newIdent, analyzer);
            }
        }
        return builder.build();
    }

    public ImmutableMap<ColumnIdent, String> analyzers() {
        Map<String, Object> propertiesMap = getNested(defaultMappingMap, "properties");
        if (propertiesMap == null) {
            return ImmutableMap.of();
        } else {
            return getAnalyzers(null, propertiesMap);
        }
    }

    public Set<Operation> supportedOperations() {
        return supportedOperations;
    }
}
