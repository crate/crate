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

package io.crate.metadata.doc;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;
import static org.elasticsearch.index.mapper.TypeParsers.DOC_VALUES;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.BitStringFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.NumberOfReplicas;
import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.Booleans;
import io.crate.common.collections.Lists2;
import io.crate.common.collections.MapBuilder;
import io.crate.common.collections.Maps;
import io.crate.expression.symbol.DefaultTraversalSymbolVisitor;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.table.Operation;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.Expression;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.StringType;

public class DocIndexMetadata {

    private static final String SETTING_CLOSED = "closed";

    private final Map<String, Object> mappingMap;
    private final Map<ColumnIdent, IndexReference.Builder> indicesBuilder = new HashMap<>();

    @SuppressWarnings("unchecked")
    private static final Comparator<Map.Entry<String, Object>> SORT_BY_POSITION_THEN_NAME = Comparator
        .comparing((Map.Entry<String, Object> e) -> {
            Map<String, Object> columnProperties = furtherColumnProperties((Map<String, Object>) e.getValue());
            return Objects.requireNonNullElse((Integer) columnProperties.get("position"), 0);
        })
        .thenComparing(Map.Entry::getKey);

    private static final Comparator<Reference> SORT_REFS_BY_POSTITON_THEN_NAME = Comparator
        .comparing(Reference::position)
        .thenComparing(o -> o.column().fqn());

    private final List<Reference> columns = new ArrayList<>();
    private final List<Reference> nestedColumns = new ArrayList<>();
    private final ArrayList<GeneratedReference> generatedColumnReferencesBuilder = new ArrayList<>();

    private final Set<Reference> droppedColumns = new HashSet<>();

    private final NodeContext nodeCtx;
    private final RelationName ident;
    private final int numberOfShards;
    private final String numberOfReplicas;
    private final Settings tableParameters;
    private final Map<String, Object> indicesMap;
    private final List<ColumnIdent> partitionedBy;
    private final Set<Operation> supportedOperations;
    private Map<ColumnIdent, IndexReference> indices;
    private List<Reference> partitionedByColumns;
    private List<GeneratedReference> generatedColumnReferences;
    private Map<ColumnIdent, Reference> references;
    @Nullable
    private String pkConstraintName;
    private List<ColumnIdent> primaryKey;
    private List<CheckConstraint<Symbol>> checkConstraints;
    private Collection<ColumnIdent> notNullColumns;
    private ColumnIdent routingCol;
    private boolean hasAutoGeneratedPrimaryKey = false;
    private boolean closed;
    Map<Integer, String> takenPositions = new HashMap<>();

    private ColumnPolicy columnPolicy = ColumnPolicy.STRICT;
    private Map<String, String> generatedColumns;

    @Nullable
    private final Version versionCreated;
    @Nullable
    private final Version versionUpgraded;

    /**
     * Analyzer used for Column Default expressions
     */
    private final ExpressionAnalyzer expressionAnalyzer;

    DocIndexMetadata(NodeContext nodeCtx, IndexMetadata metadata, RelationName ident, @Nullable PublicationsMetadata publicationsMetadata) {
        this.nodeCtx = nodeCtx;
        this.ident = ident;
        this.numberOfShards = metadata.getNumberOfShards();
        Settings settings = metadata.getSettings();
        this.numberOfReplicas = NumberOfReplicas.fromSettings(settings);
        this.mappingMap = getMappingMap(metadata);
        this.tableParameters = metadata.getSettings();

        Map<String, Object> metaMap = Maps.get(mappingMap, "_meta");
        indicesMap = Maps.getOrDefault(metaMap, "indices", Map.of());
        List<List<String>> partitionedByList = Maps.getOrDefault(metaMap, "partitioned_by", List.of());
        this.partitionedBy = getPartitionedBy(partitionedByList);
        generatedColumns = Maps.getOrDefault(metaMap, "generated_columns", Map.of());
        IndexMetadata.State state = isClosed(metadata, mappingMap, !partitionedByList.isEmpty()) ?
            IndexMetadata.State.CLOSE : IndexMetadata.State.OPEN;
        var isPublished = isPublished(ident, publicationsMetadata);
        supportedOperations = Operation.buildFromIndexSettingsAndState(metadata.getSettings(), state, isPublished);
        versionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(settings);
        versionUpgraded = settings.getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, null);
        closed = state == IndexMetadata.State.CLOSE;

        this.expressionAnalyzer = new ExpressionAnalyzer(
            CoordinatorTxnCtx.systemTransactionContext(),
            nodeCtx,
            ParamTypeHints.EMPTY,
            FieldProvider.UNSUPPORTED,
            null);
    }

    private static Map<String, Object> getMappingMap(IndexMetadata metadata) {
        MappingMetadata mappingMetadata = metadata.mapping();
        if (mappingMetadata == null) {
            return Map.of();
        }
        return mappingMetadata.sourceAsMap();
    }

    static boolean isPublished(RelationName ident, PublicationsMetadata publicationsMetadata) {
        if (publicationsMetadata != null) {
            for (var publication : publicationsMetadata.publications().values()) {
                if (publication.isForAllTables() || publication.tables().contains(ident)) {
                    return true;
                }
            }
        }
        return false;
    }

    public static boolean isClosed(IndexMetadata indexMetadata, Map<String, Object> mappingMap, boolean isPartitioned) {
        // Checking here for whether the closed flag exists on the template metadata, as partitioned tables that are
        // empty (and thus have no indexes who have states) need a way to set their state.
        if (isPartitioned) {
            return Maps.getOrDefault(
                Maps.getOrDefault(mappingMap, "_meta", null),
                SETTING_CLOSED,
                false);
        }
        return indexMetadata.getState() == IndexMetadata.State.CLOSE;
    }

    private void add(Map<String, Object> columnProperties,
                     int position,
                     long oid,
                     boolean isDropped,
                     ColumnIdent column,
                     DataType<?> type,
                     @Nullable Symbol defaultExpression,
                     ColumnPolicy columnPolicy,
                     IndexType indexType,
                     boolean nullable,
                     boolean hasDocValues) {
        Reference ref;
        boolean partitionByColumn = partitionedBy.contains(column);
        String generatedExpression = generatedColumns.get(column.fqn());
        if (partitionByColumn) {
            indexType = IndexType.PLAIN;
        }

        Reference simpleRef = new SimpleReference(
            refIdent(column),
            granularity(column),
            removeDroppedColsFromInnerTypes(columnProperties, type),
            columnPolicy,
            indexType,
            nullable,
            hasDocValues,
            position,
            oid,
            isDropped,
            defaultExpression
        );
        if (generatedExpression == null) {
            ref = simpleRef;
        } else {
            ref = new GeneratedReference(simpleRef, generatedExpression, null);
        }
        if (isDropped) {
            droppedColumns.add(ref);
            return;
        }
        if (column.isRoot()) {
            columns.add(ref);
        } else {
            nestedColumns.add(ref);
        }
        if (ref instanceof GeneratedReference genRef) {
            generatedColumnReferencesBuilder.add(genRef);
        }
    }

    private void addGeoReference(Integer position,
                                 long oid,
                                 boolean isDropped,
                                 ColumnIdent column,
                                 Symbol defaultExpression,
                                 @Nullable String tree,
                                 @Nullable String precision,
                                 @Nullable Integer treeLevels,
                                 @Nullable Double distanceErrorPct,
                                 boolean nullable,
                                 DataType<?> type) {
        Reference info = new GeoReference(
            refIdent(column),
            type,
            ColumnPolicy.DYNAMIC,
            IndexType.PLAIN,
            nullable,
            position,
            oid,
            isDropped,
            defaultExpression,
            tree,
            precision,
            treeLevels,
            distanceErrorPct
        );
        if (isDropped) {
            droppedColumns.add(info);
            return;
        }


        String generatedExpression = generatedColumns.get(column.fqn());
        if (generatedExpression != null) {
            info = new GeneratedReference(info, generatedExpression, null);
            generatedColumnReferencesBuilder.add((GeneratedReference) info);
        }

        if (column.isRoot()) {
            columns.add(info);
        } else {
            nestedColumns.add(info);
        }
    }

    private ReferenceIdent refIdent(ColumnIdent column) {
        return new ReferenceIdent(ident, column);
    }

    private RowGranularity granularity(ColumnIdent column) {
        if (partitionedBy.contains(column)) {
            return RowGranularity.PARTITION;
        }
        return RowGranularity.DOC;
    }

    record InnerObjectType(String name, int position, DataType<?> type) {}

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    @SuppressWarnings("unchecked")
    public static DataType<?> getColumnDataType(Map<String, Object> columnProperties) {
        DataType<?> type;
        String typeName = (String) columnProperties.get("type");

        if (typeName == null || ObjectType.NAME.equals(typeName)) {
            Map<String, Object> innerProperties = (Map<String, Object>) columnProperties.get("properties");
            if (innerProperties != null) {
                List<InnerObjectType> children = new ArrayList<>();
                for (Map.Entry<String, Object> entry : innerProperties.entrySet()) {
                    Map<String, Object> value = (Map<String, Object>) entry.getValue();
                    int position = (int) value.getOrDefault("position", -1);
                    children.add(new InnerObjectType(entry.getKey(), position, getColumnDataType(value)));
                }
                children.sort(Comparator.comparingInt(x -> x.position()));
                ObjectType.Builder builder = ObjectType.builder();
                for (var child : children) {
                    builder.setInnerType(child.name, child.type);
                }
                type = builder.build();
            } else {
                type = Objects.requireNonNullElse(DataTypes.ofMappingName(typeName), DataTypes.NOT_SUPPORTED);
            }
        } else if (typeName.equalsIgnoreCase("array")) {
            Map<String, Object> innerProperties = Maps.get(columnProperties, "inner");
            DataType<?> innerType = getColumnDataType(innerProperties);
            type = new ArrayType<>(innerType);
        } else {
            typeName = typeName.toLowerCase(Locale.ENGLISH);
            switch (typeName) {
                case DateFieldMapper.CONTENT_TYPE:
                    Boolean ignoreTimezone = (Boolean) columnProperties.get("ignore_timezone");
                    if (ignoreTimezone != null && ignoreTimezone) {
                        return DataTypes.TIMESTAMP;
                    } else {
                        return DataTypes.TIMESTAMPZ;
                    }
                case KeywordFieldMapper.CONTENT_TYPE:
                    Integer lengthLimit = (Integer) columnProperties.get("length_limit");
                    var blankPadding = columnProperties.get("blank_padding");
                    if (blankPadding != null && (Boolean) blankPadding) {
                        return new CharacterType(lengthLimit);
                    }
                    return lengthLimit != null
                        ? StringType.of(lengthLimit)
                        : DataTypes.STRING;

                case BitStringFieldMapper.CONTENT_TYPE:
                    Integer length = (Integer) columnProperties.get("length");
                    assert length != null : "Length is required for bit string type";
                    return new BitStringType(length);

                case FloatVectorType.NAME:
                    Integer dimensions = (Integer) columnProperties.get("dimensions");
                    return new FloatVectorType(dimensions);

                default:
                    type = Objects.requireNonNullElse(DataTypes.ofMappingName(typeName), DataTypes.NOT_SUPPORTED);
            }
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
    private static IndexType getColumnIndexType(Map<String, Object> columnProperties) {
        Object index = columnProperties.get("index");
        if (index == null) {
            if ("text".equals(columnProperties.get("type"))) {
                return IndexType.FULLTEXT;
            }
            return IndexType.PLAIN;
        }
        if (Boolean.FALSE.equals(index) || "no".equals(index) || "false".equals(index)) {
            return IndexType.NONE;
        }

        if ("not_analyzed".equals(index)) {
            return IndexType.PLAIN;
        }
        return IndexType.FULLTEXT;
    }

    private static ColumnIdent columnIdent(@Nullable ColumnIdent parent, String name) {
        if (parent == null) {
            return new ColumnIdent(name);
        }
        return parent.getChild(name);
    }

    /**
     * extracts index definitions as well
     */
    @SuppressWarnings("unchecked")
    private void internalExtractColumnDefinitions(@Nullable ColumnIdent parent,
                                                  @Nullable Map<String, Object> propertiesMap) {
        if (propertiesMap == null) {
            return;
        }

        var cols = propertiesMap.entrySet().stream().sorted(SORT_BY_POSITION_THEN_NAME)
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
                                      (e1, e2) -> e1, LinkedHashMap::new));

        for (Map.Entry<String, Object> columnEntry : cols.entrySet()) {
            Map<String, Object> columnProperties = (Map<String, Object>) columnEntry.getValue();
            final DataType<?> columnDataType = getColumnDataType(columnProperties);
            String columnName = columnEntry.getKey();
            ColumnIdent newIdent = columnIdent(parent, columnName);

            boolean nullable = !notNullColumns.contains(newIdent) && !primaryKey.contains(newIdent);
            columnProperties = furtherColumnProperties(columnProperties);
            assert columnProperties.containsKey("position") && columnProperties.get("position") != null : "Column position is missing: " + newIdent.fqn();
            // BWC compatibility with nodes < 5.1, position could be NULL if column is created on that nodes
            int position = (int) columnProperties.getOrDefault("position", 0);
            assert !takenPositions.containsKey(position) : "Duplicate column position assigned to " + newIdent.fqn() + " and " + takenPositions.get(position);
            boolean isDropped = (Boolean) columnProperties.getOrDefault("dropped", false);
            if (!isDropped) {
                // Columns, added later can get positions of the dropped columns.
                // Absolute values of the positions is not important
                // and relative positions still meet the requirement that new columns have higher positions.
                takenPositions.put(position, newIdent.fqn());
            }
            String formattedDefaultExpression = (String) columnProperties.getOrDefault("default_expr", null);
            Symbol defaultExpression = null;
            if (formattedDefaultExpression != null) {
                Expression expression = SqlParser.createExpression(formattedDefaultExpression);
                defaultExpression = this.expressionAnalyzer.convert(
                    expression,
                    new ExpressionAnalysisContext(CoordinatorTxnCtx.systemTransactionContext().sessionSettings()));
            }
            IndexType columnIndexType = getColumnIndexType(columnProperties);
            StorageSupport<?> storageSupport = columnDataType.storageSupportSafe();
            boolean docValuesDefault = storageSupport.getComputedDocValuesDefault(columnIndexType);
            boolean hasDocValues = Booleans.parseBoolean(columnProperties.getOrDefault(DOC_VALUES, docValuesDefault).toString());

            // columnProperties.getOrDefault doesn't work here for OID values fitting into int.
            // Jackson optimizes writes of small long values as stores them as ints:
            long oid = ((Number) columnProperties.getOrDefault("oid", COLUMN_OID_UNASSIGNED)).longValue();

            DataType<?> elementType = ArrayType.unnest(columnDataType);
            if (elementType.equals(DataTypes.GEO_SHAPE)) {
                String geoTree = (String) columnProperties.get("tree");
                String precision = (String) columnProperties.get("precision");
                Integer treeLevels = (Integer) columnProperties.get("tree_levels");
                Double distanceErrorPct = (Double) columnProperties.get("distance_error_pct");
                addGeoReference(
                    position,
                    oid,
                    isDropped,
                    newIdent,
                    defaultExpression,
                    geoTree,
                    precision,
                    treeLevels,
                    distanceErrorPct,
                    nullable,
                    columnDataType
                );
            } else if (columnDataType.id() == ObjectType.ID
                       || (columnDataType.id() == ArrayType.ID
                           && ((ArrayType<?>) columnDataType).innerType().id() == ObjectType.ID)) {
                ColumnPolicy columnPolicy = ColumnPolicy.fromMappingValue(columnProperties.get("dynamic"));
                add(columnProperties, position, oid, isDropped, newIdent, columnDataType, defaultExpression,
                    columnPolicy, IndexType.NONE, nullable, hasDocValues);

                if (columnProperties.get("properties") != null) {
                    // walk nested
                    internalExtractColumnDefinitions(newIdent, (Map<String, Object>) columnProperties.get("properties"));
                }
            } else if (columnDataType != DataTypes.NOT_SUPPORTED) {
                List<String> copyToColumns = Maps.get(columnProperties, "copy_to");

                // TODO: copy_to is deprecated and has to be removed after 5.4
                // extract columns this column is copied to, needed for indices
                if (copyToColumns != null) {
                    for (String copyToColumn : copyToColumns) {
                        ColumnIdent targetIdent = ColumnIdent.fromPath(copyToColumn);
                        IndexReference.Builder builder = getOrCreateIndexBuilder(targetIdent);
                        builder.addColumn(new SimpleReference(
                            refIdent(newIdent),
                            granularity(newIdent),
                            columnDataType,
                            ColumnPolicy.DYNAMIC,
                            columnIndexType,
                            nullable,
                            hasDocValues,
                            position,
                            oid,
                            isDropped,
                            defaultExpression
                        ));
                    }
                }
                // is it an index?
                var indicesKey = oid == COLUMN_OID_UNASSIGNED ? newIdent.fqn() : Long.toString(oid);
                if (indicesMap.containsKey(indicesKey)) {
                    List<String> sources = Maps.get(columnProperties, "sources");
                    if (sources != null) {
                        IndexReference.Builder builder = getOrCreateIndexBuilder(newIdent);
                        builder.indexType(columnIndexType)
                            .position(position)
                            .oid(oid)
                            .analyzer((String) columnProperties.get("analyzer"))
                            .sources(sources);
                    }
                } else {
                    add(columnProperties, position, oid, isDropped, newIdent, columnDataType, defaultExpression,
                        ColumnPolicy.DYNAMIC, columnIndexType, nullable, hasDocValues);
                }
            }
        }
    }

    /**
     * get the real column properties from a possible array mapping,
     * keeping most of this stuff inside "inner"
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> furtherColumnProperties(Map<String, Object> columnProperties) {
        if (columnProperties.get("inner") != null) {
            return (Map<String, Object>) columnProperties.get("inner");
        } else {
            return columnProperties;
        }
    }

    private IndexReference.Builder getOrCreateIndexBuilder(ColumnIdent ident) {
        return indicesBuilder.computeIfAbsent(ident, k -> new IndexReference.Builder(refIdent(ident)));
    }

    @Nullable
    private String getPkConstraintName() {
        Map<String, Object> metaMap = Maps.get(mappingMap, "_meta");
        return (metaMap != null) ? (String) metaMap.get("pk_constraint_name") : null;
    }

    private List<ColumnIdent> getPrimaryKey() {
        Map<String, Object> metaMap = Maps.get(mappingMap, "_meta");
        if (metaMap != null) {
            ArrayList<ColumnIdent> builder = new ArrayList<>();
            Object pKeys = metaMap.get("primary_keys");
            if (pKeys != null) {
                if (pKeys instanceof String str) {
                    builder.add(ColumnIdent.fromPath(str));
                    return List.copyOf(builder);
                } else if (pKeys instanceof Collection<?> keys) {
                    if (!keys.isEmpty()) {
                        for (Object pkey : keys) {
                            builder.add(ColumnIdent.fromPath(pkey.toString()));
                        }
                        return List.copyOf(builder);
                    }
                }
            }
        }
        if (getCustomRoutingCol() == null && partitionedBy.isEmpty()) {
            hasAutoGeneratedPrimaryKey = true;
            return List.of(DocSysColumns.ID);
        }
        return List.of();
    }

    private Collection<ColumnIdent> getNotNullColumns() {
        Map<String, Object> metaMap = Maps.get(mappingMap, "_meta");
        if (metaMap != null) {
            HashSet<ColumnIdent> builder = new HashSet<>();
            Map<String, Object> constraintsMap = Maps.get(metaMap, "constraints");
            if (constraintsMap != null) {
                Object notNullColumnsMeta = constraintsMap.get("not_null");
                if (notNullColumnsMeta != null) {
                    Collection<?> notNullCols = (Collection<?>) notNullColumnsMeta;
                    if (!notNullCols.isEmpty()) {
                        for (Object notNullColumn : notNullCols) {
                            builder.add(ColumnIdent.fromPath(notNullColumn.toString()));
                        }
                        return Collections.unmodifiableSet(builder);
                    }
                }
            }
        }
        return List.of();
    }

    private static List<ColumnIdent> getPartitionedBy(List<List<String>> partitionedByList) {
        ArrayList<ColumnIdent> builder = new ArrayList<>();
        for (List<String> partitionedByInfo : partitionedByList) {
            builder.add(ColumnIdent.fromPath(partitionedByInfo.get(0)));
        }
        return List.copyOf(builder);
    }

    private ColumnPolicy getColumnPolicy() {
        return ColumnPolicy.fromMappingValue(mappingMap.get("dynamic"));
    }

    private void createColumnDefinitions() {
        Map<String, Object> propertiesMap = Maps.get(mappingMap, "properties");
        internalExtractColumnDefinitions(null, propertiesMap);
    }

    private Map<ColumnIdent, IndexReference> createIndexDefinitions() {
        MapBuilder<ColumnIdent, IndexReference> builder = MapBuilder.newMapBuilder();
        for (Map.Entry<ColumnIdent, IndexReference.Builder> entry : indicesBuilder.entrySet()) {
            var indexRef = entry.getValue().build(references);
            assert indexRef.isDropped() == false : "A named index is not expected to be dropped";
            builder.put(entry.getKey(), indexRef);
        }
        indices = builder.immutableMap();
        return indices;
    }

    private ColumnIdent getCustomRoutingCol() {
        if (mappingMap != null) {
            Map<String, Object> metaMap = Maps.get(mappingMap, "_meta");
            if (metaMap != null) {
                String routingPath = (String) metaMap.get("routing");
                if (routingPath != null && !routingPath.equals(DocSysColumns.Names.ID)) {
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

    public DocIndexMetadata build() {
        notNullColumns = getNotNullColumns();
        pkConstraintName = getPkConstraintName();
        primaryKey = getPrimaryKey();
        columnPolicy = getColumnPolicy();
        // notNullColumns and primaryKey must be resolved before creating column definitions.
        createColumnDefinitions();
        references = new LinkedHashMap<>();
        DocSysColumns.forTable(ident, references::put);
        columns.sort(SORT_REFS_BY_POSTITON_THEN_NAME);
        nestedColumns.sort(SORT_REFS_BY_POSTITON_THEN_NAME);
        for (Reference ref : columns) {
            references.put(ref.column(), ref);
            for (Reference nestedColumn : nestedColumns) {
                if (nestedColumn.column().getRoot().equals(ref.column())) {
                    references.put(nestedColumn.column(), nestedColumn);
                }
            }
        }
        // createIndexDefinitions() resolves sources by FQN using references.
        // Index definition can include sub-columns, so we need to create index definitions after adding nested columns into references ^
        indices = createIndexDefinitions();
        // Order of the partitionedByColumns is important; Must be the same order as `partitionedBy` is in.
        partitionedByColumns = Lists2.map(partitionedBy, references::get);
        generatedColumnReferences = List.copyOf(generatedColumnReferencesBuilder);

        routingCol = getRoutingCol();

        Collection<Reference> refs = this.references.values();
        TableReferenceResolver tableReferenceResolver = new TableReferenceResolver(refs, ident);
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        ExpressionAnalyzer exprAnalyzer = new ExpressionAnalyzer(
            txnCtx, nodeCtx, ParamTypeHints.EMPTY, tableReferenceResolver, null);
        ExpressionAnalysisContext analysisCtx = new ExpressionAnalysisContext(txnCtx.sessionSettings());

        ArrayList<CheckConstraint<Symbol>> checkConstraintsBuilder = null;
        Map<String, Object> metaMap = Maps.get(mappingMap, "_meta");
        if (metaMap != null) {
            Map<String, String> checkConstraintsMap = Maps.get(metaMap, "check_constraints");
            if (checkConstraintsMap != null) {
                checkConstraintsBuilder = new ArrayList<>();
                for (Map.Entry<String, String> entry : checkConstraintsMap.entrySet()) {
                    String name = entry.getKey();
                    String expressionStr = entry.getValue();
                    Expression expr = SqlParser.createExpression(expressionStr);
                    Symbol analyzedExpr = exprAnalyzer.convert(expr, analysisCtx);
                    ArrayList<Short> positions = new ArrayList<>();
                    analyzedExpr.accept(RefCollector.REF_COLLECTOR_INSTANCE, positions);
                    checkConstraintsBuilder.add(new CheckConstraint<>(name, null, analyzedExpr, expressionStr, positions));
                }
            }
        }
        checkConstraints = checkConstraintsBuilder != null ? List.copyOf(checkConstraintsBuilder) : List.of();

        for (var generatedReference : generatedColumnReferences) {
            Expression expression = SqlParser.createExpression(generatedReference.formattedGeneratedExpression());
            tableReferenceResolver.references().clear();
            Symbol generatedExpression = exprAnalyzer.convert(expression, analysisCtx)
                .cast(generatedReference.valueType());
            generatedReference.generatedExpression(generatedExpression);
            generatedReference.referencedReferences(List.copyOf(tableReferenceResolver.references()));
        }
        return this;
    }

    public Map<ColumnIdent, Reference> references() {
        return references;
    }

    public Collection<Reference> columns() {
        return columns;
    }

    public Set<Reference> droppedColumns() {
        return droppedColumns;
    }

    public Map<ColumnIdent, IndexReference> indices() {
        return indices;
    }

    public List<Reference> partitionedByColumns() {
        return partitionedByColumns;
    }

    List<GeneratedReference> generatedColumnReferences() {
        return generatedColumnReferences;
    }

    Collection<ColumnIdent> notNullColumns() {
        return notNullColumns;
    }

    List<CheckConstraint<Symbol>> checkConstraints() {
        return checkConstraints;
    }

    @Nullable
    public String pkConstraintName() {
        return pkConstraintName;
    }

    public List<ColumnIdent> primaryKey() {
        return primaryKey;
    }

    ColumnIdent routingCol() {
        return routingCol;
    }

    boolean hasAutoGeneratedPrimaryKey() {
        return hasAutoGeneratedPrimaryKey;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public String numberOfReplicas() {
        return numberOfReplicas;
    }

    public List<ColumnIdent> partitionedBy() {
        return partitionedBy;
    }

    public ColumnPolicy columnPolicy() {
        return columnPolicy;
    }

    public Settings tableParameters() {
        return tableParameters;
    }

    private static class RefCollector extends DefaultTraversalSymbolVisitor<List<Short>, Void> {

        private static final RefCollector REF_COLLECTOR_INSTANCE = new RefCollector();

        @Override
        public Void visitReference(Reference reference, List<Short> context) {
            context.add(Short.valueOf((short) reference.position()));
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<ColumnIdent, String> getAnalyzers(@Nullable ColumnIdent columnIdent, Map<String, Object> propertiesMap) {
        MapBuilder<ColumnIdent, String> builder = MapBuilder.newMapBuilder();
        for (Map.Entry<String, Object> columnEntry : propertiesMap.entrySet()) {
            Map<String, Object> columnProperties = (Map<String, Object>) columnEntry.getValue();
            DataType<?> columnDataType = getColumnDataType(columnProperties);
            ColumnIdent newIdent = columnIdent(columnIdent, columnEntry.getKey());
            columnProperties = furtherColumnProperties(columnProperties);
            if (columnDataType.id() == ObjectType.ID
                || (columnDataType.id() == ArrayType.ID
                    && ((ArrayType<?>) columnDataType).innerType().id() == ObjectType.ID)) {
                if (columnProperties.get("properties") != null) {
                    builder.putAll(getAnalyzers(newIdent, (Map<String, Object>) columnProperties.get("properties")));
                }
            }
            String analyzer = (String) columnProperties.get("analyzer");
            if (analyzer != null) {
                builder.put(newIdent, analyzer);
            }
        }
        return builder.map();
    }

    Map<ColumnIdent, String> analyzers() {
        Map<String, Object> propertiesMap = Maps.get(mappingMap, "properties");
        if (propertiesMap == null) {
            return Map.of();
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
     * Even though referenced marked as "dropped" are excluded from {@link #columns}, when a sub-column
     * of an object col is dropped, the {@link ObjectType#innerTypes()} still contain those dropped sub-columns.
     */
    @SuppressWarnings("unchecked")
    private DataType<?> removeDroppedColsFromInnerTypes(Map<String, Object> columnProperties, DataType<?> type) {
        if (type.id() == ObjectType.ID) {
            ObjectType.Builder builder = new ObjectType.Builder();
            for (var entry : ((ObjectType) type).innerTypes().entrySet()) {
                Map<String, Object> innerProps =
                    ((Map<String, Object>)((Map<String, Object>) columnProperties.get("properties"))
                        .get(entry.getKey()));
                if (innerProps.get("dropped") == null || (Boolean) innerProps.get("dropped") == false) {
                    builder.setInnerType(entry.getKey(), removeDroppedColsFromInnerTypes(innerProps, entry.getValue()));
                }
            }
            return builder.build();
        } else if (type.id() == ArrayType.ID) {
            Map<String, Object> innerProps = (Map<String, Object>) columnProperties.get("inner");
            if (innerProps == null) {
                innerProps = columnProperties;
            }
            var newInnerType = removeDroppedColsFromInnerTypes(innerProps, ((ArrayType<?>) type).innerType());
            return new ArrayType<>(newInnerType);
        }
        return type;
    }
}
