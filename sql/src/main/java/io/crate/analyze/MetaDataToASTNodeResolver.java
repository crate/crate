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

package io.crate.analyze;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.CrateTableOption;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GenericProperty;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.ObjectColumnType;
import io.crate.sql.tree.PartitionedBy;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.TableElement;
import io.crate.types.ArrayType;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.SetType;
import org.elasticsearch.common.Nullable;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

public class MetaDataToASTNodeResolver {

    public static CreateTable resolveCreateTable(DocTableInfo info) {
        Extractor extractor = new Extractor(info);
        return extractor.extractCreateTable();
    }

    private static class Extractor {

        private final DocTableInfo tableInfo;

        public Extractor(DocTableInfo tableInfo) {
            this.tableInfo = tableInfo;
        }

        private Table extractTable() {
            return new Table(QualifiedName.of(tableInfo.ident().fqn()), false);
        }

        private List<TableElement> extractTableElements() {
            List<TableElement> elements = new ArrayList<>();
            // column definitions
            elements.addAll(extractColumnDefinitions(null));
            // primary key constraint
            PrimaryKeyConstraint pk = extractPrimaryKeyConstraint();
            if (pk != null) elements.add(pk);
            // index definitions
            elements.addAll(extractIndexDefinitions());
            return elements;
        }

        private List<ColumnDefinition> extractColumnDefinitions(@Nullable ColumnIdent parent) {
            Iterator<Reference> referenceIterator = tableInfo.iterator();
            List<ColumnDefinition> elements = new ArrayList<>();
            while (referenceIterator.hasNext()) {
                Reference info = referenceIterator.next();
                ColumnIdent ident = info.column();
                if (ident.isSystemColumn()) continue;
                if (parent != null && !ident.isChildOf(parent)) continue;
                if (parent == null && !ident.path().isEmpty()) continue;
                if (parent != null) {
                    if (ident.getParent().compareTo(parent) > 0) continue;
                }

                ColumnType columnType;
                if (info.valueType().equals(DataTypes.OBJECT)) {
                    columnType = new ObjectColumnType(info.columnPolicy().value(), extractColumnDefinitions(ident));
                } else if (info.valueType().id() == ArrayType.ID) {
                    DataType innerType = ((CollectionType) info.valueType()).innerType();
                    ColumnType innerColumnType;
                    if (innerType.equals(DataTypes.OBJECT)) {
                        innerColumnType = new ObjectColumnType(info.columnPolicy().value(), extractColumnDefinitions(ident));
                    } else {
                        innerColumnType = new ColumnType(innerType.getName());
                    }
                    columnType = CollectionColumnType.array(innerColumnType);
                } else if (info.valueType().id() == SetType.ID) {
                    ColumnType innerColumnType = new ColumnType(((CollectionType) info.valueType()).innerType().getName());
                    columnType = CollectionColumnType.set(innerColumnType);
                } else {
                    columnType = new ColumnType(info.valueType().getName());
                }

                List<ColumnConstraint> constraints = new ArrayList<>();
                if (!info.isNullable()) {
                    constraints.add(new NotNullColumnConstraint());
                }
                if (info.indexType().equals(Reference.IndexType.NO)
                    && !info.valueType().equals(DataTypes.OBJECT)
                    && !(info.valueType().id() == ArrayType.ID &&
                         ((CollectionType) info.valueType()).innerType().equals(DataTypes.OBJECT))) {
                    constraints.add(IndexColumnConstraint.OFF);
                } else if (info.indexType().equals(Reference.IndexType.ANALYZED)) {
                    String analyzer = tableInfo.getAnalyzerForColumnIdent(ident);
                    GenericProperties properties = new GenericProperties();
                    if (analyzer != null) {
                        properties.add(new GenericProperty(FulltextAnalyzerResolver.CustomType.ANALYZER.getName(), new StringLiteral(analyzer)));
                    }
                    constraints.add(new IndexColumnConstraint("fulltext", properties));
                } else if (info.valueType().equals(DataTypes.GEO_SHAPE)) {
                    GeoReference geoReference = (GeoReference) info;
                    GenericProperties properties = new GenericProperties();
                    if (geoReference.distanceErrorPct() != null) {
                        properties.add(new GenericProperty("distance_error_pct", StringLiteral.fromObject(geoReference.distanceErrorPct())));
                    }
                    if (geoReference.precision() != null) {
                        properties.add(new GenericProperty("precision", StringLiteral.fromObject(geoReference.precision())));
                    }
                    if (geoReference.treeLevels() != null) {
                        properties.add(new GenericProperty("tree_levels", StringLiteral.fromObject(geoReference.treeLevels())));
                    }
                    constraints.add(new IndexColumnConstraint(geoReference.geoTree(), properties));
                }
                Expression expression = null;
                if (info instanceof GeneratedReference) {
                    String formattedExpression = ((GeneratedReference) info).formattedGeneratedExpression();
                    expression = SqlParser.createExpression(formattedExpression);
                }

                if (info.isColumnStoreDisabled()) {
                    GenericProperties properties = new GenericProperties();
                    properties.add(new GenericProperty("columnstore", BooleanLiteral.fromObject(false)));
                    constraints.add(new ColumnStorageDefinition(properties));
                }

                String columnName = ident.isTopLevel() ? ident.name() : ident.path().get(ident.path().size() - 1);
                ColumnDefinition column = new ColumnDefinition(columnName, expression, columnType, constraints);
                elements.add(column);
            }
            return elements;
        }

        private PrimaryKeyConstraint extractPrimaryKeyConstraint() {
            if (!tableInfo.primaryKey().isEmpty()) {
                if (tableInfo.primaryKey().size() == 1 && tableInfo.primaryKey().get(0).isSystemColumn()) {
                    return null;
                }
                return new PrimaryKeyConstraint(expressionsFromColumns(tableInfo.primaryKey()));
            }
            return null;
        }

        private List<IndexDefinition> extractIndexDefinitions() {
            List<IndexDefinition> elements = new ArrayList<>();
            Iterator indexColumns = tableInfo.indexColumns();
            if (indexColumns != null) {
                while (indexColumns.hasNext()) {
                    IndexReference indexRef = (IndexReference) indexColumns.next();
                    String name = indexRef.column().name();
                    List<Expression> columns = expressionsFromReferences(indexRef.columns());
                    if (indexRef.indexType().equals(Reference.IndexType.ANALYZED)) {
                        String analyzer = indexRef.analyzer();
                        GenericProperties properties = new GenericProperties();
                        if (analyzer != null) {
                            properties.add(new GenericProperty(FulltextAnalyzerResolver.CustomType.ANALYZER.getName(), new StringLiteral(analyzer)));
                        }
                        elements.add(new IndexDefinition(name, "fulltext", columns, properties));
                    } else if (indexRef.indexType().equals(Reference.IndexType.NOT_ANALYZED)) {
                        elements.add(new IndexDefinition(name, "plain", columns, GenericProperties.EMPTY));
                    }
                }
            }
            return elements;
        }

        private List<CrateTableOption> extractTableOptions() {
            List<CrateTableOption> options = new ArrayList<>();
            // CLUSTERED BY (...) INTO ... SHARDS
            Expression clusteredByExpression = null;
            ColumnIdent clusteredBy = tableInfo.clusteredBy();
            if (clusteredBy != null && !clusteredBy.isSystemColumn()) {
                clusteredByExpression = expressionFromColumn(clusteredBy);
            }
            Expression numShards = new LongLiteral(Integer.toString(tableInfo.numberOfShards()));
            options.add(new ClusteredBy(Optional.ofNullable(clusteredByExpression), Optional.of(numShards)));
            // PARTITIONED BY (...)
            if (tableInfo.isPartitioned() && !tableInfo.partitionedBy().isEmpty()) {
                options.add(new PartitionedBy(expressionsFromColumns(tableInfo.partitionedBy())));
            }
            return options;
        }

        private GenericProperties extractTableProperties() {
            // WITH ( key = value, ... )
            GenericProperties properties = new GenericProperties();
            Expression numReplicas = new StringLiteral(tableInfo.numberOfReplicas().utf8ToString());
            properties.add(new GenericProperty(
                    TablePropertiesAnalyzer.esToCrateSettingName(TableParameterInfo.NUMBER_OF_REPLICAS),
                    numReplicas
                )
            );
            // we want a sorted map of table parameters
            TreeMap<String, Object> tableParameters = new TreeMap<>();
            tableParameters.putAll(tableInfo.tableParameters());
            for (Map.Entry<String, Object> entry : tableParameters.entrySet()) {
                properties.add(new GenericProperty(
                        TablePropertiesAnalyzer.esToCrateSettingName(entry.getKey()),
                        Literal.fromObject(entry.getValue())
                    )
                );
            }
            properties.add(new GenericProperty(
                "column_policy",
                new StringLiteral(tableInfo.columnPolicy().value())
            ));
            return properties;
        }


        private CreateTable extractCreateTable() {
            Table table = extractTable();
            List<TableElement> tableElements = extractTableElements();
            List<CrateTableOption> tableOptions = extractTableOptions();
            return new CreateTable(table, tableElements, tableOptions, extractTableProperties(), true);
        }

        private Expression expressionFromColumn(ColumnIdent ident) {
            Expression fqn = new QualifiedNameReference(QualifiedName.of(ident.getRoot().fqn()));
            for (String child : ident.path()) {
                fqn = new SubscriptExpression(fqn, Literal.fromObject(child));
            }
            return fqn;
        }

        private List<Expression> expressionsFromReferences(List<Reference> columns) {
            List<Expression> expressions = new ArrayList<>(columns.size());
            for (Reference ident : columns) {
                expressions.add(expressionFromColumn(ident.column()));
            }
            return expressions;
        }

        private List<Expression> expressionsFromColumns(List<ColumnIdent> columns) {
            List<Expression> expressions = new ArrayList<>(columns.size());
            for (ColumnIdent ident : columns) {
                expressions.add(expressionFromColumn(ident));
            }
            return expressions;
        }

    }
}
