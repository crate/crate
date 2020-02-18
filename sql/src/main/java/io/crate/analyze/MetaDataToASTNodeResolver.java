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

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CollectionColumnType;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
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
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;

import static io.crate.analyze.TableParameters.stripIndexPrefix;

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

        private Table<Expression> extractTable() {
            return new Table<>(QualifiedName.of(tableInfo.ident().fqn()), false);
        }

        private List<TableElement<Expression>> extractTableElements() {
            List<TableElement<Expression>> elements = new ArrayList<>();
            // column definitions
            elements.addAll(extractColumnDefinitions(null));
            // primary key constraint
            PrimaryKeyConstraint pk = extractPrimaryKeyConstraint();
            if (pk != null) elements.add(pk);
            // index definitions
            elements.addAll(extractIndexDefinitions());
            tableInfo.checkConstraints()
                .stream()
                .map(chk -> new CheckConstraint<>(
                    chk.name(),
                    chk.columnName(),
                    SqlParser.createExpression(chk.expressionStr()),
                    chk.expressionStr()))
                .forEach(elements::add);
            return elements;
        }

        private List<ColumnDefinition<Expression>> extractColumnDefinitions(@Nullable ColumnIdent parent) {
            Iterator<Reference> referenceIterator = tableInfo.iterator();
            List<ColumnDefinition<Expression>> elements = new ArrayList<>();
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
                if (info.valueType().id() == ObjectType.ID) {
                    columnType = new ObjectColumnType<>(info.columnPolicy().name(), extractColumnDefinitions(ident));
                } else if (info.valueType().id() == ArrayType.ID) {
                    DataType innerType = ((ArrayType) info.valueType()).innerType();
                    ColumnType innerColumnType;
                    if (innerType.id() == ObjectType.ID) {
                        innerColumnType = new ObjectColumnType<>(info.columnPolicy().name(), extractColumnDefinitions(ident));
                    } else {
                        innerColumnType = new ColumnType(innerType.getName());
                    }
                    columnType = new CollectionColumnType(innerColumnType);
                } else {
                    columnType = new ColumnType(info.valueType().getName());
                }

                List<ColumnConstraint<Expression>> constraints = new ArrayList<>();
                if (!info.isNullable()) {
                    constraints.add(new NotNullColumnConstraint<>());
                }
                if (info.indexType().equals(Reference.IndexType.NO)
                    && info.valueType().id() != ObjectType.ID
                    && !(info.valueType().id() == ArrayType.ID &&
                         ((ArrayType) info.valueType()).innerType().id() == ObjectType.ID)) {
                    constraints.add(IndexColumnConstraint.off());
                } else if (info.indexType().equals(Reference.IndexType.ANALYZED)) {
                    String analyzer = tableInfo.getAnalyzerForColumnIdent(ident);
                    GenericProperties<Expression> properties = new GenericProperties<>();
                    if (analyzer != null) {
                        properties.add(new GenericProperty<>(FulltextAnalyzerResolver.CustomType.ANALYZER.getName(), new StringLiteral(analyzer)));
                    }
                    constraints.add(new IndexColumnConstraint<>("fulltext", properties));
                } else if (info.valueType().equals(DataTypes.GEO_SHAPE)) {
                    GeoReference geoReference = (GeoReference) info;
                    GenericProperties<Expression> properties = new GenericProperties<>();
                    if (geoReference.distanceErrorPct() != null) {
                        properties.add(new GenericProperty<>("distance_error_pct", StringLiteral.fromObject(geoReference.distanceErrorPct())));
                    }
                    if (geoReference.precision() != null) {
                        properties.add(new GenericProperty<>("precision", StringLiteral.fromObject(geoReference.precision())));
                    }
                    if (geoReference.treeLevels() != null) {
                        properties.add(new GenericProperty<>("tree_levels", StringLiteral.fromObject(geoReference.treeLevels())));
                    }
                    constraints.add(new IndexColumnConstraint<>(geoReference.geoTree(), properties));
                }

                Expression generatedExpression = null;
                if (info instanceof GeneratedReference) {
                    String formattedExpression = ((GeneratedReference) info).formattedGeneratedExpression();
                    generatedExpression = SqlParser.createExpression(formattedExpression);
                }
                Expression defaultExpression = null;
                Symbol defaultExpr = info.defaultExpression();
                if (defaultExpr != null) {
                    String symbol = defaultExpr.toString(Style.UNQUALIFIED);
                    defaultExpression = SqlParser.createExpression(symbol);
                }

                if (info.isColumnStoreDisabled()) {
                    GenericProperties<Expression> properties = new GenericProperties<>();
                    properties.add(new GenericProperty<>("columnstore", BooleanLiteral.fromObject(false)));
                    constraints.add(new ColumnStorageDefinition<>(properties));
                }

                String columnName = ident.isTopLevel() ? ident.name() : ident.path().get(ident.path().size() - 1);
                elements.add(new ColumnDefinition<>(
                    columnName,
                    defaultExpression,
                    generatedExpression,
                    columnType,
                    constraints)
                );
            }
            return elements;
        }

        private PrimaryKeyConstraint<Expression> extractPrimaryKeyConstraint() {
            if (!tableInfo.primaryKey().isEmpty()) {
                if (tableInfo.primaryKey().size() == 1 && tableInfo.primaryKey().get(0).isSystemColumn()) {
                    return null;
                }
                return new PrimaryKeyConstraint<>(expressionsFromColumns(tableInfo.primaryKey()));
            }
            return null;
        }

        private List<IndexDefinition<Expression>> extractIndexDefinitions() {
            List<IndexDefinition<Expression>> elements = new ArrayList<>();
            Iterator indexColumns = tableInfo.indexColumns();
            if (indexColumns != null) {
                while (indexColumns.hasNext()) {
                    IndexReference indexRef = (IndexReference) indexColumns.next();
                    String name = indexRef.column().name();
                    List<Expression> columns = expressionsFromReferences(indexRef.columns());
                    if (indexRef.indexType().equals(Reference.IndexType.ANALYZED)) {
                        String analyzer = indexRef.analyzer();
                        GenericProperties<Expression> properties = new GenericProperties<>();
                        if (analyzer != null) {
                            properties.add(new GenericProperty<>(FulltextAnalyzerResolver.CustomType.ANALYZER.getName(), new StringLiteral(analyzer)));
                        }
                        elements.add(new IndexDefinition<>(name, "fulltext", columns, properties));
                    } else if (indexRef.indexType().equals(Reference.IndexType.NOT_ANALYZED)) {
                        elements.add(new IndexDefinition<>(name, "plain", columns, GenericProperties.empty()));
                    }
                }
            }
            return elements;
        }

        private Optional<PartitionedBy<Expression>> createPartitionedBy() {
            if (tableInfo.partitionedBy().isEmpty()) {
                return Optional.empty();
            } else {
                return Optional.of(new PartitionedBy<>(expressionsFromColumns(tableInfo.partitionedBy())));
            }
        }

        private Optional<ClusteredBy<Expression>> createClusteredBy() {
            ColumnIdent clusteredByColumn = tableInfo.clusteredBy();
            Expression clusteredBy = clusteredByColumn == null || clusteredByColumn.isSystemColumn()
                ? null
                : expressionFromColumn(clusteredByColumn);
            Expression numShards = new LongLiteral(Integer.toString(tableInfo.numberOfShards()));
            return Optional.of(new ClusteredBy<>(Optional.ofNullable(clusteredBy), Optional.of(numShards)));
        }

        private GenericProperties<Expression> extractTableProperties() {
            // WITH ( key = value, ... )
            GenericProperties<Expression> properties = new GenericProperties<>();
            Expression numReplicas = new StringLiteral(tableInfo.numberOfReplicas());
            properties.add(new GenericProperty<>(
                TableParameters.NUMBER_OF_REPLICAS.getKey(),
                numReplicas
                )
            );
            // we want a sorted map of table parameters
            TreeMap<String, Object> tableParameters = new TreeMap<>(tableInfo.parameters());
            for (Map.Entry<String, Object> entry : tableParameters.entrySet()) {
                properties.add(new GenericProperty<>(
                        stripIndexPrefix(entry.getKey()),
                        Literal.fromObject(entry.getValue())
                    )
                );
            }
            properties.add(new GenericProperty<>(
                "column_policy",
                new StringLiteral(tableInfo.columnPolicy().lowerCaseName())
            ));
            return properties;
        }


        private CreateTable<Expression> extractCreateTable() {
            Table<Expression> table = extractTable();
            List<TableElement<Expression>> tableElements = extractTableElements();
            Optional<PartitionedBy<Expression>> partitionedBy = createPartitionedBy();
            Optional<ClusteredBy<Expression>> clusteredBy = createClusteredBy();
            return new CreateTable<>(table, tableElements, partitionedBy, clusteredBy, extractTableProperties(), true);
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
