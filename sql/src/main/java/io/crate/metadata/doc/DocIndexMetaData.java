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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import io.crate.Constants;
import io.crate.Version;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.TableParameterInfo;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.core.collections.Maps;
import io.crate.exceptions.TableAliasSchemaException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexMappings;
import io.crate.metadata.IndexReference;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
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
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class DocIndexMetaData {

    private static final String ID = "_id";

    private static final String SETTING_CLOSED = "closed";

    private final IndexMetaData metaData;
    private final Map<String, Object> mappingMap;
    private final Map<ColumnIdent, IndexReference.Builder> indicesBuilder = new HashMap<>();

    private final ImmutableSortedSet.Builder<Reference> columnsBuilder = ImmutableSortedSet.orderedBy(
        Comparator.comparing(o -> o.ident().columnIdent().fqn()));

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
    private ImmutableList<Reference> columns;
    private ImmutableMap<ColumnIdent, IndexReference> indices;
    private ImmutableList<Reference> partitionedByColumns;
    private ImmutableList<GeneratedReference> generatedColumnReferences;
    private ImmutableMap<ColumnIdent, Reference> references;
    private ImmutableList<ColumnIdent> primaryKey;
    private ImmutableCollection<ColumnIdent> notNullColumns;
    private ColumnIdent routingCol;
    private ImmutableList<ColumnIdent> partitionedBy;
    private final boolean isAlias;
    private final Set<String> aliases;
    private boolean hasAutoGeneratedPrimaryKey = false;
    private boolean closed;

    private ColumnPolicy columnPolicy = ColumnPolicy.DYNAMIC;
    private Map<String, String> generatedColumns;

    @Nullable
    private final Version versionCreated;
    @Nullable
    private final Version versionUpgraded;

    DocIndexMetaData(Functions functions, IndexMetaData metaData, TableIdent ident) throws IOException {
        this.functions = functions;
        this.ident = ident;
        this.metaData = metaData;
        this.isAlias = !metaData.getIndex().getName().equals(ident.indexName());
        this.numberOfShards = metaData.getNumberOfShards();
        Settings settings = metaData.getSettings();
        this.numberOfReplicas = NumberOfReplicas.fromSettings(settings);
        this.aliases = ImmutableSet.copyOf(metaData.getAliases().keys().toArray(String.class));
        this.mappingMap = getMappingMap(metaData);
        this.tableParameters = TableParameterInfo.tableParametersFromIndexMetaData(metaData);

        Map<String, Object> metaMap = Maps.getNested(mappingMap, "_meta");
        indicesMap = Maps.getNested(metaMap, "indices", ImmutableMap.<String, Object>of());
        partitionedByList = Maps.getNested(metaMap, "partitioned_by", ImmutableList.<List<String>>of());
        generatedColumns = Maps.getNested(metaMap, "generated_columns", ImmutableMap.<String, String>of());
        IndexMetaData.State state = isClosed(metaData, mappingMap, !partitionedByList.isEmpty()) ?
            IndexMetaData.State.CLOSE : IndexMetaData.State.OPEN;
        if (isAlias && partitionedByList.isEmpty()) {
            supportedOperations = Operation.SYS_READ_ONLY;
        } else {
            supportedOperations = Operation.buildFromIndexSettingsAndState(metaData.getSettings(), state);
        }
        versionCreated = getVersionCreated(mappingMap);
        versionUpgraded = getVersionUpgraded(mappingMap);
        closed = state == IndexMetaData.State.CLOSE;
    }

    private static Map<String, Object> getMappingMap(IndexMetaData metaData) throws IOException {
        MappingMetaData mappingMetaData = metaData.mappingOrDefault(Constants.DEFAULT_MAPPING_TYPE);
        if (mappingMetaData == null) {
            return ImmutableMap.of();
        }
        return mappingMetaData.sourceAsMap();
    }

    private static Map<String, Object> getVersionMap(Map<String, Object> mappingMap) {
        return Maps.getNested(
            Maps.getNested(mappingMap, "_meta", null),
            IndexMappings.VERSION_STRING,
            null);
    }

    public static String getRoutingHashFunction(Map<String, Object> mappingMap) {
        return Maps.getNested(
            Maps.getNested(mappingMap, "_meta", null),
            IndexMappings.SETTING_ROUTING_HASH_FUNCTION,
            IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION);
    }

    public static String getRoutingHashFunctionPrettyName(String routingHashFunction) {
        return IndexMappings.routingHashFunctionPrettyNameLookupMap.getOrDefault(routingHashFunction,
            IndexMappings.DEFAULT_ROUTING_HASH_FUNCTION);
    }

    @Nullable
    public static Version getVersionCreated(Map<String, Object> mappingMap) {
        Map<String, Object> versionMap = getVersionMap(mappingMap);
        return Version.fromMap(Maps.getNested(versionMap, Version.Property.CREATED.toString(), null));
    }

    @Nullable
    public static Version getVersionUpgraded(Map<String, Object> mappingMap) {
        Map<String, Object> versionMap = getVersionMap(mappingMap);
        return Version.fromMap(Maps.getNested(versionMap, Version.Property.UPGRADED.toString(), null));
    }

    public static boolean isClosed(IndexMetaData indexMetaData, Map<String, Object> mappingMap, boolean isPartitioned) {
        // Checking here for whether the closed flag exists on the template metadata, as partitioned tables that are
        // empty (and thus have no indexes who have states) need a way to set their state.
        if (isPartitioned) {
            return Maps.getNested(
                Maps.getNested(mappingMap, "_meta", null),
                SETTING_CLOSED,
                false);
        }
        return indexMetaData.getState() == IndexMetaData.State.CLOSE;
    }

    private void addPartitioned(ColumnIdent column, DataType type, boolean isNotNull) {
        add(column, type, ColumnPolicy.DYNAMIC, Reference.IndexType.NOT_ANALYZED, true, isNotNull);
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

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    static DataType getColumnDataType(Map<String, Object> columnProperties) {
        DataType type;
        String typeName = (String) columnProperties.get("type");

        if (typeName == null) {
            if (columnProperties.containsKey("properties")) {
                type = DataTypes.OBJECT;
            } else {
                return DataTypes.NOT_SUPPORTED;
            }
        } else if (typeName.equalsIgnoreCase("array")) {

            Map<String, Object> innerProperties = Maps.getNested(columnProperties, "inner");
            DataType innerType = getColumnDataType(innerProperties);
            type = new ArrayType(innerType);
        } else {
            typeName = typeName.toLowerCase(Locale.ENGLISH);
            type = MoreObjects.firstNonNull(DataTypes.ofMappingName(typeName), DataTypes.NOT_SUPPORTED);
        }
        return type;
    }

    /**
     * Get the IndexType from columnProperties.
     * <br />
     * Properties might look like:
     * <pre>
     *     {
     *         "type": "integer"
     *     }
     *
     *
     *     {
     *         "type": "text",
     *         "analyzer": "english"
     *     }
     *
     *
     *     {
     *          "type": "text",
     *          "fields": {
     *              "keyword": {
     *                  "type": "keyword",
     *                  "ignore_above": "256"
     *              }
     *          }
     *     }
     *
     *     {
     *         "type": "date",
     *         "index": "no"
     *     }
     *
     *     {
     *          "type": "keyword",
     *          "index": false
     *     }
     * </pre>
     */
    private static Reference.IndexType getColumnIndexType(Map<String, Object> columnProperties) {
        Object index = columnProperties.get("index");
        if (index == null) {
            if ("text".equals(columnProperties.get("type"))) {
                return Reference.IndexType.ANALYZED;
            }
            return Reference.IndexType.NOT_ANALYZED;
        }
        if (Boolean.FALSE.equals(index) || "no".equals(index) || "false".equals(index)) {
            return Reference.IndexType.NO;
        }

        if ("not_analyzed".equals(index)) {
            return Reference.IndexType.NOT_ANALYZED;
        }
        return Reference.IndexType.ANALYZED;
    }

    private static ColumnIdent childIdent(@Nullable ColumnIdent ident, String name) {
        if (ident == null) {
            return new ColumnIdent(name);
        }
        return ColumnIdent.getChild(ident, name);
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
                List<String> copyToColumns = Maps.getNested(columnProperties, "copy_to");

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
        return indicesBuilder.computeIfAbsent(ident, k -> new IndexReference.Builder(refIdent(ident)));
    }

    private ImmutableList<ColumnIdent> getPrimaryKey() {
        Map<String, Object> metaMap = Maps.getNested(mappingMap, "_meta");
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
        Map<String, Object> metaMap = Maps.getNested(mappingMap, "_meta");
        if (metaMap != null) {
            ImmutableSet.Builder<ColumnIdent> builder = ImmutableSet.builder();
            Map<String, Object> constraintsMap = Maps.getNested(metaMap, "constraints");
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
        return ColumnPolicy.of(mappingMap.get("dynamic"));
    }

    private void createColumnDefinitions() {
        Map<String, Object> propertiesMap = Maps.getNested(mappingMap, "properties");
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
            ColumnIdent columnIdent = partitioned.v1();
            DataType dataType = partitioned.v2();
            addPartitioned(columnIdent, dataType, !notNullColumns.contains(columnIdent));
        }
    }

    private ColumnIdent getCustomRoutingCol() {
        if (mappingMap != null) {
            Map<String, Object> metaMap = Maps.getNested(mappingMap, "_meta");
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
        if (generatedColumnReferences.isEmpty()) {
            return;
        }
        Collection<Reference> references = this.references.values();
        TableReferenceResolver tableReferenceResolver = new TableReferenceResolver(references);
        ExpressionAnalyzer expressionAnalyzer = new ExpressionAnalyzer(
            functions, SessionContext.SYSTEM_SESSION, ParamTypeHints.EMPTY, tableReferenceResolver, null);
        ExpressionAnalysisContext context = new ExpressionAnalysisContext();
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
        DocSysColumns.forTable(ident, referencesBuilder::put);
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

    ImmutableList<GeneratedReference> generatedColumnReferences() {
        return generatedColumnReferences;
    }

    ImmutableCollection<ColumnIdent> notNullColumns() {
        return notNullColumns;
    }

    public ImmutableList<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    ColumnIdent routingCol() {
        return routingCol;
    }

    String getRoutingHashFunction() {
        return getRoutingHashFunction(mappingMap);
    }

    /**
     * Returns true if the schema of this and <code>other</code> is the same,
     * this includes the table name, as this is reflected in the ReferenceIdents of
     * the columns.
     */
    boolean schemaEquals(DocIndexMetaData other) {
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
            boolean mappingChanged = XContentHelper.update(this.mappingMap, other.mappingMap, true);
            if (mappingChanged) {
                IndexMetaData indexMetaData = mergeIndexMetaData(other.metaData);
                DocIndexMetaData ret = new DocIndexMetaData(functions, indexMetaData, other.ident).build();
                updateTemplate(ret, transportPutIndexTemplateAction, this.metaData.getSettings());
                return ret;
            }
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
            .mapping(Constants.DEFAULT_MAPPING_TYPE, md.mappingMap)
            .create(false)
            .settings(updateSettings.filter(s ->
                s.equals(IndexMetaData.SETTING_VERSION_CREATED) == false))
            .template(templateName + "*");
        for (String alias : md.aliases()) {
            request = request.alias(new Alias(alias));
        }
        transportPutIndexTemplateAction.execute(request);
    }

    /**
     * @return the name of the underlying index even if this table is referenced by alias
     */
    String concreteIndexName() {
        return metaData.getIndex().getName();
    }

    public boolean isAlias() {
        return isAlias;
    }

    public Set<String> aliases() {
        return aliases;
    }

    boolean hasAutoGeneratedPrimaryKey() {
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

    ImmutableMap<ColumnIdent, String> analyzers() {
        Map<String, Object> propertiesMap = Maps.getNested(mappingMap, "properties");
        if (propertiesMap == null) {
            return ImmutableMap.of();
        } else {
            return getAnalyzers(null, propertiesMap);
        }
    }

    Set<Operation> supportedOperations() {
        return supportedOperations;
    }

    @Nullable
    public Version versionCreated() {
        return versionCreated;
    }

    @Nullable
    public Version versionUpgraded() {
        return versionUpgraded;
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Merges this {@link IndexMetaData} with the given one,
     * but comparing to {@link IndexMetaData#builder(IndexMetaData)}, it won't set {@link IndexMetaData#primaryTerms}
     * as the number of shards can differ (its applied via the settings).
     */
    private IndexMetaData mergeIndexMetaData(IndexMetaData other) {
        IndexMetaData.Builder builder = IndexMetaData.builder(other.getIndex().getName())
            .settings(this.metaData.getSettings())
            .state(other.getState())
            .version(other.getVersion());

        // mappings are expected to be merged already, so we use this one's
        try {
            builder.putMapping(new MappingMetaData(Constants.DEFAULT_MAPPING_TYPE, mappingMap));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        for (ObjectObjectCursor<String, AliasMetaData> cursor : other.getAliases()) {
            builder.putAlias(cursor.value);
        }
        for (ObjectObjectCursor<String, IndexMetaData.Custom> cursor : other.getCustoms()) {
            builder.putCustom(cursor.key, cursor.value);
        }
        return builder.build();
    }
}
