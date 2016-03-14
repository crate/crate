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

import io.crate.metadata.*;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.*;
import io.crate.types.*;
import org.elasticsearch.common.Nullable;

import java.util.*;

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
            Iterator<ReferenceInfo> referenceInfoIterator = tableInfo.iterator();
            List<ColumnDefinition> elements = new ArrayList<>();
            while (referenceInfoIterator.hasNext()) {
                ReferenceInfo info = referenceInfoIterator.next();
                ColumnIdent ident = info.ident().columnIdent();
                if (ident.isSystemColumn()) continue;
                if (parent != null && !ident.isChildOf(parent)) continue;
                if (parent == null && !ident.path().isEmpty()) continue;
                if (parent != null) {
                    if (ident.getParent().compareTo(parent) > 0) continue;
                }

                ColumnType columnType = null;
                if (info.type().equals(DataTypes.OBJECT)) {
                    columnType = new ObjectColumnType(info.columnPolicy().value(), extractColumnDefinitions(ident));
                } else if (info.type().id() == ArrayType.ID) {
                    DataType innerType = ((CollectionType)info.type()).innerType();
                    ColumnType innerColumnType = null;
                    if (innerType.equals(DataTypes.OBJECT)) {
                        innerColumnType = new ObjectColumnType(info.columnPolicy().value(), extractColumnDefinitions(ident));
                    } else {
                        innerColumnType = new ColumnType(innerType.getName());
                    }
                    columnType = CollectionColumnType.array(innerColumnType);
                } else if (info.type().id() == SetType.ID) {
                    ColumnType innerColumnType = new ColumnType(((CollectionType) info.type()).innerType().getName());
                    columnType = CollectionColumnType.set(innerColumnType);
                } else {
                    columnType = new ColumnType(info.type().getName());
                }

                String columnName = ident.isColumn() ? ident.name() : ident.path().get(ident.path().size()-1);
                List<ColumnConstraint> constraints = new ArrayList<>();
                if (info.indexType().equals(ReferenceInfo.IndexType.NO)
                        && !info.type().equals(DataTypes.OBJECT)
                        && !(info.type().id() == ArrayType.ID && ((CollectionType)info.type()).innerType().equals(DataTypes.OBJECT))) {
                    constraints.add(IndexColumnConstraint.OFF);
                } else if (info.indexType().equals(ReferenceInfo.IndexType.ANALYZED)) {
                    String analyzer = tableInfo.getAnalyzerForColumnIdent(ident);
                    GenericProperties properties = new GenericProperties();
                    if (analyzer != null) {
                        properties.add(new GenericProperty(FulltextAnalyzerResolver.CustomType.ANALYZER.getName(), new StringLiteral(analyzer)));
                    }
                    constraints.add(new IndexColumnConstraint("fulltext", properties));
                } else if (info.type().equals(DataTypes.GEO_SHAPE)) {
                    GeoReferenceInfo geoReferenceInfo = (GeoReferenceInfo)info;
                    GenericProperties properties = new GenericProperties();
                    if (geoReferenceInfo.distanceErrorPct() != null) {
                        properties.add(new GenericProperty("distance_error_pct", StringLiteral.fromObject(geoReferenceInfo.distanceErrorPct())));
                    }
                    if (geoReferenceInfo.precision() != null) {
                        properties.add(new GenericProperty("precision", StringLiteral.fromObject(geoReferenceInfo.precision())));
                    }
                    if (geoReferenceInfo.treeLevels() != null) {
                        properties.add(new GenericProperty("tree_levels", StringLiteral.fromObject(geoReferenceInfo.treeLevels())));
                    }
                    constraints.add(new IndexColumnConstraint(geoReferenceInfo.geoTree(), properties));
                }
                Expression expression = null;
                if (info instanceof GeneratedReferenceInfo) {
                    String formattedExpression = ((GeneratedReferenceInfo) info).formattedGeneratedExpression();
                    expression = SqlParser.createExpression(formattedExpression);
                }

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
                    IndexReferenceInfo indexRef = (IndexReferenceInfo) indexColumns.next();
                    String name = indexRef.ident().columnIdent().name();
                    List<Expression> columns = expressionsFromReferenceInfos(indexRef.columns());
                    if (indexRef.indexType().equals(ReferenceInfo.IndexType.ANALYZED)) {
                        String analyzer = indexRef.analyzer();
                        GenericProperties properties = new GenericProperties();
                        if (analyzer != null) {
                            properties.add(new GenericProperty(FulltextAnalyzerResolver.CustomType.ANALYZER.getName(), new StringLiteral(analyzer)));
                        }
                        elements.add(new IndexDefinition(name, "fulltext", columns, properties));
                    } else if (indexRef.indexType().equals(ReferenceInfo.IndexType.NOT_ANALYZED)) {
                        elements.add(new IndexDefinition(name, "plain", columns, null));
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
            options.add(new ClusteredBy(clusteredByExpression, numShards));
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

        private boolean extractIfNotExists() {
            return true;
        }

        private CreateTable extractCreateTable() {
            Table table = extractTable();
            List<TableElement> tableElements = extractTableElements();
            List<CrateTableOption> tableOptions = extractTableOptions();
            GenericProperties tableProperties = extractTableProperties();
            boolean ifNotExists = extractIfNotExists();
            return new CreateTable(table, tableElements, tableOptions, tableProperties, ifNotExists);
        }

        private Expression expressionFromColumn(ColumnIdent ident) {
            Expression fqn = new QualifiedNameReference(QualifiedName.of(ident.getRoot().fqn()));
            for (String child : ident.path()) {
                fqn = new SubscriptExpression(fqn, Literal.fromObject(child));
            }
            return fqn;
        }

        private List<Expression> expressionsFromReferenceInfos(List<ReferenceInfo> columns) {
            List<Expression> expressions = new ArrayList<>(columns.size());
            for (ReferenceInfo ident : columns) {
                expressions.add(expressionFromColumn(ident.ident().columnIdent()));
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
