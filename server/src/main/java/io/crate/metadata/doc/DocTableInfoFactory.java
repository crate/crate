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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.mapper.BitStringFieldMapper;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.mapper.TypeParsers;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.expressions.TableReferenceResolver;
import io.crate.analyze.relations.FieldProvider;
import io.crate.common.Booleans;
import io.crate.common.collections.Maps;
import io.crate.exceptions.RelationUnknown;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.table.Operation;
import io.crate.replication.logical.metadata.PublicationsMetadata;
import io.crate.server.xcontent.XContentHelper;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.StringType;

@Singleton
public class DocTableInfoFactory {

    private static final Logger LOGGER = LogManager.getLogger(DocTableInfoFactory.class);

    private final NodeContext nodeCtx;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final CoordinatorTxnCtx systemTransactionContext;

    @Inject
    public DocTableInfoFactory(NodeContext nodeCtx) {
        this.nodeCtx = nodeCtx;
        this.systemTransactionContext = CoordinatorTxnCtx.systemTransactionContext();
        this.expressionAnalyzer = new ExpressionAnalyzer(
            systemTransactionContext,
            nodeCtx,
            ParamTypeHints.EMPTY,
            FieldProvider.UNSUPPORTED,
            null
        );
    }

    public DocTableInfo create(RelationName relation, Metadata metadata) {
        String templateName = PartitionName.templateName(relation.schema(), relation.name());
        IndexTemplateMetadata indexTemplateMetadata = metadata.templates().get(templateName);
        Version versionCreated;
        Version versionUpgraded;
        Map<String, Object> mappingSource;
        Settings tableParameters;
        IndexMetadata.State state;
        String[] concreteIndices;
        try {
            concreteIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                indexTemplateMetadata == null
                    ? IndicesOptions.strictExpandOpen()
                    : IndicesOptions.lenientExpandOpen(),
                relation.indexNameOrAlias()
            );
        } catch (IndexNotFoundException e) {
            throw new RelationUnknown(relation.fqn(), e);
        }
        String[] concreteOpenIndices;
        List<PartitionName> partitions;
        if (indexTemplateMetadata == null) {
            IndexMetadata index = metadata.index(relation.indexNameOrAlias());
            if (index == null) {
                if (concreteIndices.length > 0) {
                    index = metadata.index(concreteIndices[0]);
                    LOGGER.info(
                        "Found indices={} for relation={} without index template. Orphaned partition?",
                        concreteIndices,
                        relation
                    );
                }
                if (index == null) {
                    throw new RelationUnknown(relation);
                }
            }
            tableParameters = index.getSettings();
            versionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(tableParameters);
            versionUpgraded = tableParameters.getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, null);
            state = index.getState();
            MappingMetadata mapping = index.mapping();
            mappingSource = mapping == null ? Map.of() : mapping.sourceAsMap();
            concreteOpenIndices = concreteIndices;
            if (concreteIndices.length == 0) {
                throw new RelationUnknown(relation);
            }
            partitions = List.of();
        } else {
            mappingSource = XContentHelper.toMap(
                indexTemplateMetadata.mapping().compressedReference(),
                XContentType.JSON
            );
            mappingSource = Maps.getOrDefault(mappingSource, "default", mappingSource);
            tableParameters = indexTemplateMetadata.settings();
            versionCreated = tableParameters.getAsVersion(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
            versionUpgraded = null;
            boolean isClosed = Maps.getOrDefault(
                Maps.getOrDefault(mappingSource, "_meta", Map.of()), "closed", false);
            state = isClosed ? State.CLOSE : State.OPEN;
            // We need all concrete open indices, as closed indices must not appear in the routing.
            concreteOpenIndices = IndexNameExpressionResolver.concreteIndexNames(
                metadata,
                IndicesOptions.fromOptions(true, true, true, false, IndicesOptions.strictExpandOpenAndForbidClosed()),
                relation.indexNameOrAlias()
            );
            partitions = new ArrayList<>(concreteIndices.length);
            for (String indexName : concreteIndices) {
                partitions.add(PartitionName.fromIndexOrTemplate(indexName));
            }

        }
        final Map<String, Object> metaMap = Maps.getOrDefault(mappingSource, "_meta", Map.of());
        final List<ColumnIdent> partitionedBy = parsePartitionedByStringsList(
            Maps.getOrDefault(metaMap, "partitioned_by", List.of())
        );
        List<ColumnIdent> primaryKeys = getPrimaryKeys(metaMap);
        Set<ColumnIdent> notNullColumns = getNotNullColumns(metaMap);

        Map<String, Object> indicesMap = Maps.getOrDefault(metaMap, "indices", Map.of());
        Map<String, Object> properties = Maps.getOrDefault(mappingSource, "properties", Map.of());
        Map<ColumnIdent, Reference> references = new HashMap<>();
        Map<ColumnIdent, IndexReference.Builder> indexColumns = new HashMap<>();
        Map<ColumnIdent, String> analyzers = new HashMap<>();

        parseColumns(
            relation,
            null,
            indicesMap,
            notNullColumns,
            primaryKeys,
            partitionedBy,
            properties,
            indexColumns,
            analyzers,
            references
        );
        var refExpressionAnalyzer = new ExpressionAnalyzer(
            systemTransactionContext,
            nodeCtx,
            ParamTypeHints.EMPTY,
            new TableReferenceResolver(references, relation),
            null
        );
        var expressionAnalysisContext = new ExpressionAnalysisContext(systemTransactionContext.sessionSettings());
        Map<String, String> generatedColumns = Maps.getOrDefault(metaMap, "generated_columns", Map.of());
        for (Entry<String,String> entry : generatedColumns.entrySet()) {
            ColumnIdent column = ColumnIdent.fromPath(entry.getKey());
            String generatedExpressionStr = entry.getValue();
            Reference reference = references.get(column);
            Symbol generatedExpression = refExpressionAnalyzer.convert(
                SqlParser.createExpression(generatedExpressionStr),
                expressionAnalysisContext
            ).cast(reference.valueType());
            assert reference != null : "Column present in generatedColumns must exist";
            GeneratedReference generatedRef = new GeneratedReference(
                reference,
                generatedExpressionStr,
                generatedExpression
            );
            references.put(column, generatedRef);
        }
        List<CheckConstraint<Symbol>> checkConstraints = getCheckConstraints(
            refExpressionAnalyzer,
            expressionAnalysisContext,
            metaMap
        );
        PublicationsMetadata publicationsMetadata = metadata.custom(PublicationsMetadata.TYPE);
        ColumnIdent clusteredBy = getClusteredBy(primaryKeys, Maps.get(metaMap, "routing"));
        return new DocTableInfo(
            relation,
            references,
            indexColumns.entrySet().stream()
                .collect(Collectors.toMap(Entry::getKey, e -> e.getValue().build(references))),
            analyzers,
            Maps.get(metaMap, "pk_constraint_name"),
            primaryKeys,
            checkConstraints,
            clusteredBy,
            concreteIndices,
            concreteOpenIndices,
            tableParameters,
            partitionedBy,
            partitions,
            ColumnPolicy.fromMappingValue(mappingSource.get("dynamic")),
            versionCreated,
            versionUpgraded,
            state == IndexMetadata.State.CLOSE,
            Operation.buildFromIndexSettingsAndState(
                tableParameters,
                state,
                publicationsMetadata == null ? false : publicationsMetadata.isPublished(relation)
            )
        );
    }

    private static ColumnIdent getClusteredBy(List<ColumnIdent> primaryKeys, @Nullable String routing) {
        if (routing != null) {
            return ColumnIdent.fromPath(routing);
        }
        if (primaryKeys.size() == 1) {
            return primaryKeys.get(0);
        }
        return DocSysColumns.ID;
    }

    private static List<CheckConstraint<Symbol>> getCheckConstraints(
            ExpressionAnalyzer expressionAnalyzer,
            ExpressionAnalysisContext expressionAnalysisContext,
            Map<String, Object> metaMap) {
        Map<String, String> checkConstraints = Maps.get(metaMap, "check_constraints");
        if (checkConstraints == null) {
            return List.of();
        }
        List<CheckConstraint<Symbol>> result = new ArrayList<>(checkConstraints.size());
        for (Entry<String,String> entry : checkConstraints.entrySet()) {
            String name = entry.getKey();
            String expressionStr = entry.getValue();
            Symbol expression = expressionAnalyzer.convert(
                SqlParser.createExpression(expressionStr),
                expressionAnalysisContext
            );
            var checkConstraint = new CheckConstraint<>(name, expression, expressionStr);
            result.add(checkConstraint);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private void parseColumns(RelationName relationName,
                              @Nullable ColumnIdent parent,
                              Map<String, Object> indicesMap,
                              Set<ColumnIdent> notNullColumns,
                              List<ColumnIdent> primaryKeys,
                              List<ColumnIdent> partitionedBy,
                              Map<String, Object> properties,
                              Map<ColumnIdent, IndexReference.Builder> indexColumns,
                              Map<ColumnIdent, String> analyzers,
                              Map<ColumnIdent, Reference> references) {
        for (Entry<String,Object> entry : properties.entrySet()) {
            String columnName = entry.getKey();
            Map<String, Object> columnProperties = (Map<String, Object>) entry.getValue();
            DataType<?> type = getColumnDataType(columnProperties);
            ColumnIdent column = parent == null ? new ColumnIdent(columnName) : parent.getChild(columnName);
            ReferenceIdent refIdent = new ReferenceIdent(relationName, column);
            columnProperties = innerProperties(columnProperties);

            String analyzer = (String) columnProperties.get("analyzer");
            if (analyzer != null) {
                analyzers.put(column, analyzer);
            }

            Symbol defaultExpression = parseExpression(Maps.get(columnProperties, "default_expr"));
            boolean isPartitionColumn = partitionedBy.contains(column);
            IndexType indexType = isPartitionColumn
                ? IndexType.PLAIN
                : getColumnIndexType(columnProperties);
            RowGranularity granularity = isPartitionColumn
                ? RowGranularity.PARTITION
                : RowGranularity.DOC;

            StorageSupport<?> storageSupport = type.storageSupportSafe();
            boolean docValuesDefault = storageSupport.getComputedDocValuesDefault(indexType);
            Object docValues = columnProperties.get(TypeParsers.DOC_VALUES);
            boolean hasDocValues = docValues == null
                ? docValuesDefault
                : Booleans.parseBoolean(docValues.toString());

            int position = Maps.getOrDefault(columnProperties, "position", 0);
            Number oidNum = Maps.getOrDefault(columnProperties, "oid", Metadata.COLUMN_OID_UNASSIGNED);
            long oid = oidNum.longValue();
            DataType<?> elementType = ArrayType.unnest(type);

            boolean isDropped = Maps.getOrDefault(columnProperties, "dropped", false);
            boolean nullable = !notNullColumns.contains(column) && !primaryKeys.contains(column);

            if (elementType.equals(DataTypes.GEO_SHAPE)) {
                String geoTree = (String) columnProperties.get("tree");
                String precision = (String) columnProperties.get("precision");
                Integer treeLevels = (Integer) columnProperties.get("tree_levels");
                Double distanceErrorPct = (Double) columnProperties.get("distance_error_pct");
                Reference ref = new GeoReference(
                    refIdent,
                    type,
                    ColumnPolicy.DYNAMIC,
                    IndexType.PLAIN,
                    nullable,
                    position,
                    oid,
                    isDropped,
                    defaultExpression,
                    geoTree,
                    precision,
                    treeLevels,
                    distanceErrorPct
                );
                references.put(column, ref);
            } else if (elementType.id() == ObjectType.ID) {
                ColumnPolicy columnPolicy = ColumnPolicy.fromMappingValue(columnProperties.get("dynamic"));

                Reference ref = new SimpleReference(
                    refIdent,
                    granularity,
                    type,
                    columnPolicy,
                    indexType,
                    nullable,
                    hasDocValues,
                    position,
                    oid,
                    isDropped,
                    defaultExpression
                );
                references.put(column, ref);

                Map<String, Object> nestedProperties = Maps.get(columnProperties, "properties");
                if (nestedProperties != null) {
                    parseColumns(
                        relationName,
                        column,
                        indicesMap,
                        notNullColumns,
                        primaryKeys,
                        partitionedBy,
                        nestedProperties,
                        indexColumns,
                        analyzers,
                        references
                    );
                }
            } else if (type != DataTypes.NOT_SUPPORTED) {
                List<String> copyToColumns = Maps.get(columnProperties, "copy_to");
                // TODO: copy_to is deprecated and has to be removed after 5.4
                // extract columns this column is copied to, needed for indices
                if (copyToColumns != null) {
                    for (String copyToColumn : copyToColumns) {
                        ColumnIdent targetIdent = ColumnIdent.fromPath(copyToColumn);
                        IndexReference.Builder builder = indexColumns.computeIfAbsent(
                            targetIdent,
                            k -> new IndexReference.Builder(refIdent)
                        );
                        builder.addColumn(new SimpleReference(
                            refIdent,
                            granularity,
                            type,
                            ColumnPolicy.DYNAMIC,
                            indexType,
                            nullable,
                            hasDocValues,
                            position,
                            oid,
                            isDropped,
                            defaultExpression
                        ));
                    }
                }

                var indicesKey = oid == Metadata.COLUMN_OID_UNASSIGNED ? column.fqn() : Long.toString(oid);
                if (indicesMap.containsKey(indicesKey)) {
                    List<String> sources = Maps.get(columnProperties, "sources");
                    if (sources != null) {
                        IndexReference.Builder builder = indexColumns.computeIfAbsent(
                            column,
                            k -> new IndexReference.Builder(refIdent)
                        );
                        builder.indexType(indexType)
                            .position(position)
                            .oid(oid)
                            .analyzer(analyzer)
                            .sources(sources);
                    }
                } else {
                    Reference ref;
                    if (analyzer == null) {
                        ref = new SimpleReference(
                            refIdent,
                            granularity,
                            type,
                            ColumnPolicy.DYNAMIC,
                            indexType,
                            nullable,
                            hasDocValues,
                            position,
                            oid,
                            isDropped,
                            defaultExpression
                        );
                    } else {
                        ref = new IndexReference(
                            refIdent,
                            granularity,
                            type,
                            ColumnPolicy.DYNAMIC,
                            indexType,
                            nullable,
                            hasDocValues,
                            position,
                            oid,
                            isDropped,
                            defaultExpression,
                            List.of(),
                            analyzer
                        );
                    }
                    references.put(column, ref);
                }
            }
        }
    }


    @Nullable
    private Symbol parseExpression(@Nullable String expression) {
        if (expression == null) {
            return null;
        }
        return expressionAnalyzer.convert(
            SqlParser.createExpression(expression),
            new ExpressionAnalysisContext(systemTransactionContext.sessionSettings())
        );
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

    /**
     * Extract `inner` if present, otherwise properties as is.
     * Array types have the mapping for their inner type within `inner`
     **/
    private static Map<String, Object> innerProperties(Map<String, Object> columnProperties) {
        Map<String, Object> inner = Maps.get(columnProperties, "inner");
        return inner == null ? columnProperties : inner;
    }

    private static List<ColumnIdent> getPrimaryKeys(Map<String, Object> metaMap) {
        Object primaryKeys = metaMap.get("primary_keys");
        if (primaryKeys == null) {
            return List.of();
        }
        if (primaryKeys instanceof String pkString) {
            return List.of(ColumnIdent.fromPath(pkString));
        }
        if (primaryKeys instanceof Collection<?> keys) {
            List<ColumnIdent> result = new ArrayList<>(keys.size());
            for (Object key : keys) {
                result.add(ColumnIdent.fromPath(key.toString()));
            }
            return result;
        }
        return List.of();
    }

    private static Set<ColumnIdent> getNotNullColumns(Map<String, Object> metaMap) {
        Map<String, Object> constraintsMap = Maps.get(metaMap, "constraints");
        if (constraintsMap == null) {
            return Set.of();
        }
        HashSet<ColumnIdent> result = new HashSet<>();
        Collection<Object> notNullCols = Maps.getOrDefault(constraintsMap, "not_null", List.of());
        for (Object notNullColumn : notNullCols) {
            result.add(ColumnIdent.fromPath(notNullColumn.toString()));
        }
        return result;
    }

    private static List<ColumnIdent> parsePartitionedByStringsList(List<List<String>> partitionedByList) {
        ArrayList<ColumnIdent> builder = new ArrayList<>();
        for (List<String> partitionedByInfo : partitionedByList) {
            builder.add(ColumnIdent.fromPath(partitionedByInfo.get(0)));
        }
        return List.copyOf(builder);
    }

    record InnerObjectType(String name, int position, DataType<?> type) {}

    /**
     * extract dataType from given columnProperties
     *
     * @param columnProperties map of String to Object containing column properties
     * @return dataType of the column with columnProperties
     */
    @SuppressWarnings("unchecked")
    private static DataType<?> getColumnDataType(Map<String, Object> columnProperties) {
        String typeName = (String) columnProperties.get("type");

        if (typeName == null || ObjectType.NAME.equals(typeName)) {
            Map<String, Object> innerProperties = (Map<String, Object>) columnProperties.get("properties");
            if (innerProperties == null) {
                return Objects.requireNonNullElse(DataTypes.ofMappingName(typeName), DataTypes.NOT_SUPPORTED);
            }

            List<InnerObjectType> children = new ArrayList<>();
            for (Map.Entry<String, Object> entry : innerProperties.entrySet()) {
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                boolean isDropped = Maps.getOrDefault(value, "dropped", false);
                if (!isDropped) {
                    int position = (int) value.getOrDefault("position", -1);
                    children.add(new InnerObjectType(entry.getKey(), position, getColumnDataType(value)));
                }
            }
            children.sort(Comparator.comparingInt(x -> x.position()));
            ObjectType.Builder builder = ObjectType.builder();
            for (var child : children) {
                builder.setInnerType(child.name, child.type);
            }
            return builder.build();
        }

        if (typeName.equalsIgnoreCase("array")) {
            Map<String, Object> innerProperties = Maps.get(columnProperties, "inner");
            DataType<?> innerType = getColumnDataType(innerProperties);
            return new ArrayType<>(innerType);
        }

        return switch (typeName.toLowerCase(Locale.ENGLISH)) {
            case DateFieldMapper.CONTENT_TYPE -> {
                Boolean ignoreTimezone = (Boolean) columnProperties.get("ignore_timezone");
                if (ignoreTimezone != null && ignoreTimezone) {
                    yield DataTypes.TIMESTAMP;
                } else {
                    yield DataTypes.TIMESTAMPZ;
                }
            }
            case KeywordFieldMapper.CONTENT_TYPE -> {
                Integer lengthLimit = (Integer) columnProperties.get("length_limit");
                var blankPadding = columnProperties.get("blank_padding");
                if (blankPadding != null && (Boolean) blankPadding) {
                    yield new CharacterType(lengthLimit);
                }
                yield lengthLimit != null
                    ? StringType.of(lengthLimit)
                    : DataTypes.STRING;
            }
            case BitStringFieldMapper.CONTENT_TYPE -> {
                Integer length = (Integer) columnProperties.get("length");
                assert length != null : "Length is required for bit string type";
                yield new BitStringType(length);
            }
            case FloatVectorType.NAME -> {
                Integer dimensions = (Integer) columnProperties.get("dimensions");
                yield new FloatVectorType(dimensions);
            }
            default -> Objects.requireNonNullElse(DataTypes.ofMappingName(typeName), DataTypes.NOT_SUPPORTED);
        };
    }
}
