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
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata.State;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;
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
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.BitStringType;
import io.crate.types.CharacterType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.FloatVectorType;
import io.crate.types.NumericType;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;
import io.crate.types.StringType;
import io.crate.types.UndefinedType;

public class DocTableInfoFactory {

    private final NodeContext nodeCtx;
    private final ExpressionAnalyzer expressionAnalyzer;
    private final CoordinatorTxnCtx systemTransactionContext;

    public static class MappingKeys {
        public static final String DOC_VALUES = "doc_values";
        public static final String DATE = "date";
        public static final String KEYWORD = "keyword";
        public static final String BITSTRING = "bit";
    }

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

    public void validateSchema(IndexMetadata indexMetadata) {
    }

    public DocTableInfo create(RelationName relation, Metadata metadata) {
        RelationMetadata relationMetadata = metadata.getRelation(relation);
        if (relationMetadata instanceof RelationMetadata.Table table) {
            DocTableInfo newTableInfo = tableFromRelationMetadata(table);
            return newTableInfo;
        }
        throw new RelationUnknown(relation);
    }

    private DocTableInfo tableFromRelationMetadata(RelationMetadata.Table table) {
        Map<ColumnIdent, Reference> columns = table.columns().stream()
            .filter(ref -> !(ref instanceof IndexReference indexRef && !indexRef.columns().isEmpty()))
            .collect(Collectors.toMap(ref -> ref.column(), ref -> ref));
        Map<ColumnIdent, IndexReference> indexColumns = table.columns().stream()
            .filter(ref -> ref instanceof IndexReference indexRef && !indexRef.columns().isEmpty())
            .map(ref -> (IndexReference) ref)
            .collect(Collectors.toMap(ref -> ref.column(), ref -> ref));

        var expressionAnalyzer = new ExpressionAnalyzer(
            systemTransactionContext,
            nodeCtx,
            ParamTypeHints.EMPTY,
            new TableReferenceResolver(columns, table.name()),
            null
        );
        var expressionAnalysisContext = new ExpressionAnalysisContext(systemTransactionContext.sessionSettings());

        Version versionCreated = IndexMetadata.SETTING_INDEX_VERSION_CREATED.get(table.settings());
        Version versionUpgraded = table.settings().getAsVersion(IndexMetadata.SETTING_VERSION_UPGRADED, null);
        ColumnIdent routingColumn = table.routingColumn();
        if (routingColumn == null) {
            routingColumn = table.primaryKeys().size() == 1
                ? table.primaryKeys().get(0)
                : SysColumns.ID.COLUMN;
        }
        List<CheckConstraint<Symbol>> checkConstraints = getCheckConstraints(
            expressionAnalyzer,
            expressionAnalysisContext,
            table.checkConstraints()
        );
        return new DocTableInfo(
            table.name(),
            columns,
            indexColumns,
            table.pkConstraintName(),
            table.primaryKeys(),
            checkConstraints,
            routingColumn,
            table.settings(),
            table.partitionedBy(),
            table.columnPolicy(),
            versionCreated,
            versionUpgraded,
            table.state() == State.CLOSE,
            Operation.buildFromIndexSettingsAndState(
                table.settings(),
                table.state(),
                false // TODO: publicationsMetadata == null ? false : publicationsMetadata.isPublished(relation)
            ),
            0
        );
    }

    private static List<CheckConstraint<Symbol>> getCheckConstraints(
            ExpressionAnalyzer expressionAnalyzer,
            ExpressionAnalysisContext expressionAnalysisContext,
            @Nullable Map<String, String> checkConstraints) {
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
    public static void parseColumns(ExpressionAnalyzer expressionAnalyzer,
                                    RelationName relationName,
                                    @Nullable ColumnIdent parent,
                                    Map<String, Object> indicesMap,
                                    Set<ColumnIdent> notNullColumns,
                                    List<ColumnIdent> primaryKeys,
                                    List<ColumnIdent> partitionedBy,
                                    Map<String, Object> properties,
                                    Map<ColumnIdent, IndexReference.Builder> indexColumns,
                                    Map<ColumnIdent, Reference> references) {
        CoordinatorTxnCtx txnCtx = CoordinatorTxnCtx.systemTransactionContext();
        for (Entry<String,Object> entry : properties.entrySet()) {
            String columnName = entry.getKey();
            Map<String, Object> columnProperties = (Map<String, Object>) entry.getValue();
            final DataType<?> type = getColumnDataType(columnProperties);
            ColumnIdent column = parent == null ? ColumnIdent.of(columnName) : parent.getChild(columnName);
            ReferenceIdent refIdent = new ReferenceIdent(relationName, column);
            columnProperties = innerProperties(columnProperties);

            String analyzer = (String) columnProperties.get("analyzer");
            String defaultExpressionString = Maps.get(columnProperties, "default_expr");
            Symbol defaultExpression = null;
            if (defaultExpressionString != null) {
                defaultExpression = expressionAnalyzer.convert(
                    SqlParser.createExpression(defaultExpressionString),
                    new ExpressionAnalysisContext(txnCtx.sessionSettings())
                );
            }
            boolean isPartitionColumn = partitionedBy.contains(column);
            IndexType indexType = isPartitionColumn
                ? IndexType.PLAIN
                : getColumnIndexType(columnProperties);
            RowGranularity granularity = isPartitionColumn
                ? RowGranularity.PARTITION
                : RowGranularity.DOC;

            StorageSupport<?> storageSupport = type.storageSupportSafe();
            boolean docValuesDefault = storageSupport.getComputedDocValuesDefault(indexType);
            Object docValues = columnProperties.get(MappingKeys.DOC_VALUES);
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
                Reference ref = new SimpleReference(
                    refIdent,
                    granularity,
                    type,
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
                        expressionAnalyzer,
                        relationName,
                        column,
                        indicesMap,
                        notNullColumns,
                        primaryKeys,
                        partitionedBy,
                        nestedProperties,
                        indexColumns,
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
        var inner = columnProperties;
        var next = inner;
        while (next != null) {
            inner = next;
            next = Maps.get(inner, "inner");
        }
        return inner;
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
        String typeName = (String) columnProperties.get("type");

        if (typeName == null || ObjectType.NAME.equals(typeName)) {
            Map<String, Object> innerProperties = (Map<String, Object>) columnProperties.getOrDefault("properties", Map.of());
            List<InnerObjectType> children = new ArrayList<>();
            for (Map.Entry<String, Object> entry : innerProperties.entrySet()) {
                Map<String, Object> value = (Map<String, Object>) entry.getValue();
                boolean isDropped = Maps.getOrDefault(value, "dropped", false);
                if (!isDropped) {
                    int position = (int) value.getOrDefault("position", -1);
                    children.add(new InnerObjectType(entry.getKey(), position, getColumnDataType(value)));
                }
            }
            children.sort(Comparator.comparingInt(InnerObjectType::position));
            ObjectType.Builder builder = ObjectType.of(ColumnPolicy.fromMappingValue(columnProperties.get("dynamic")));
            for (var child : children) {
                builder.setInnerType(child.name, child.type);
            }
            return builder.build();
        }

        if (typeName.equalsIgnoreCase("array")) {
            Map<String, Object> innerProperties = Maps.get(columnProperties, "inner");
            if (Objects.equals(UndefinedType.INSTANCE.getName(), innerProperties.get("type"))) {
                return new ArrayType<>(UndefinedType.INSTANCE);
            }
            DataType<?> innerType = getColumnDataType(innerProperties);
            return new ArrayType<>(innerType);
        }

        return switch (typeName.toLowerCase(Locale.ENGLISH)) {
            case MappingKeys.DATE -> {
                Boolean ignoreTimezone = (Boolean) columnProperties.get("ignore_timezone");
                if (ignoreTimezone != null && ignoreTimezone) {
                    yield DataTypes.TIMESTAMP;
                } else {
                    yield DataTypes.TIMESTAMPZ;
                }
            }
            case MappingKeys.KEYWORD -> {
                Integer lengthLimit = (Integer) columnProperties.get("length_limit");
                var blankPadding = columnProperties.get("blank_padding");
                if (blankPadding != null && (Boolean) blankPadding) {
                    yield CharacterType.of(lengthLimit);
                }
                yield lengthLimit != null
                    ? StringType.of(lengthLimit)
                    : DataTypes.STRING;
            }
            case MappingKeys.BITSTRING -> {
                Integer length = (Integer) columnProperties.get("length");
                assert length != null : "Length is required for bit string type";
                yield new BitStringType(length);
            }
            case NumericType.NAME -> {
                Integer precision = (Integer) columnProperties.get("precision");
                Integer scale = (Integer) columnProperties.get("scale");
                yield new NumericType(precision, scale);
            }
            case FloatVectorType.NAME -> {
                Integer dimensions = (Integer) columnProperties.get("dimensions");
                yield new FloatVectorType(dimensions);
            }
            default -> Objects.requireNonNullElse(DataTypes.ofMappingName(typeName), DataTypes.NOT_SUPPORTED);
        };
    }
}
