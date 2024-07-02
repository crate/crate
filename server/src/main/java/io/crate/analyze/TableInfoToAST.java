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

package io.crate.analyze;


import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.format.Style;
import io.crate.fdw.ForeignTable;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.FulltextAnalyzerResolver;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.GeoReference;
import io.crate.metadata.IndexReference;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.NumberOfReplicas;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.CreateForeignTable;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.DefaultConstraint;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GeneratedExpressionConstraint;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.Literal;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.PartitionedBy;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Statement;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.Table;
import io.crate.sql.tree.TableElement;
import io.crate.types.ArrayType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import io.crate.types.StorageSupport;


public class TableInfoToAST {

    private final TableInfo tableInfo;

    public TableInfoToAST(TableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    public Statement toStatement() {
        QualifiedName name = QualifiedName.of(tableInfo.ident().fqn());
        List<TableElement<Expression>> tableElements = extractTableElements();
        if (tableInfo instanceof DocTableInfo docTable) {
            Table<Expression> table = new Table<>(name, false);
            Optional<PartitionedBy<Expression>> partitionedBy = createPartitionedBy(docTable);
            Optional<ClusteredBy<Expression>> clusteredBy = createClusteredBy(docTable);
            return new CreateTable<>(
                table,
                tableElements,
                partitionedBy,
                clusteredBy,
                extractTableProperties(),
                true
            );
        } else if (tableInfo instanceof ForeignTable foreignTable) {
            Map<String, Expression> options = new HashMap<>();
            for (var entry : foreignTable.options().getAsStructuredMap().entrySet()) {
                String optionName = entry.getKey();
                Object optionValue = entry.getValue();
                options.put(optionName, Literal.fromObject(optionValue));
            }
            return new CreateForeignTable(
                name,
                true,
                tableElements,
                foreignTable.server(),
                options
            );
        } else {
            throw new UnsupportedOperationException(
                "Cannot convert " + tableInfo.getClass().getSimpleName() + " to AST");
        }
    }

    private List<TableElement<Expression>> extractTableElements() {
        List<TableElement<Expression>> elements = new ArrayList<>();
        // column definitions
        elements.addAll(extractColumnDefinitions(null));
        // primary key constraint
        PrimaryKeyConstraint<Expression> pk = extractPrimaryKeyConstraint();
        if (pk != null) {
            elements.add(pk);
        }
        // index definitions
        elements.addAll(extractIndexDefinitions());
        tableInfo.checkConstraints()
            .stream()
            .map(chk -> new CheckConstraint<>(
                chk.name(),
                SqlParser.createExpression(chk.expressionStr()),
                chk.expressionStr()))
            .forEach(elements::add);
        return elements;
    }

    private List<ColumnDefinition<Expression>> extractColumnDefinitions(@Nullable ColumnIdent parent) {
        Iterator<Reference> referenceIterator = tableInfo.iterator();
        List<ColumnDefinition<Expression>> elements = new ArrayList<>();
        while (referenceIterator.hasNext()) {
            Reference ref = referenceIterator.next();
            ColumnIdent ident = ref.column();
            if (ident.isSystemColumn()) {
                continue;
            }
            if (parent != null && !ident.isChildOf(parent)) {
                continue;
            }
            if (parent == null && !ident.path().isEmpty()) {
                continue;
            }
            if (parent != null) {
                if (ident.getParent().compareTo(parent) > 0) {
                    continue;
                }
            }

            final ColumnType<Expression> columnType = ref.valueType().toColumnType(
                ref.columnPolicy(),
                () -> extractColumnDefinitions(ident)
            );
            List<ColumnConstraint<Expression>> constraints = new ArrayList<>();

            if (ref instanceof GeneratedReference generatedRef) {
                String formattedExpression = generatedRef.formattedGeneratedExpression();
                Expression generatedExpression = SqlParser.createExpression(formattedExpression);
                constraints.add(new GeneratedExpressionConstraint<>(null, generatedExpression, formattedExpression));
            }
            Symbol defaultExpr = ref.defaultExpression();
            if (defaultExpr != null) {
                String symbol = defaultExpr.toString(Style.UNQUALIFIED);
                Expression defaultExpression = SqlParser.createExpression(symbol);
                constraints.add(new DefaultConstraint<>(null, defaultExpression, symbol));
            }

            if (!ref.isNullable()) {
                constraints.add(new NotNullColumnConstraint<>());
            }
            if (ref.indexType().equals(IndexType.NONE)
                && ref.valueType().id() != ObjectType.ID
                && !(ref.valueType().id() == ArrayType.ID &&
                        ((ArrayType<?>) ref.valueType()).innerType().id() == ObjectType.ID)) {
                constraints.add(IndexColumnConstraint.off());
            } else if (ref.indexType().equals(IndexType.FULLTEXT)) {
                String analyzer = ((DocTableInfo) tableInfo).getAnalyzerForColumnIdent(ident);
                GenericProperties<Expression> properties;
                if (analyzer == null) {
                    properties = GenericProperties.empty();
                } else {
                    properties = new GenericProperties<>(Map.of(
                        FulltextAnalyzerResolver.CustomType.ANALYZER.getName(),
                        new StringLiteral(analyzer)
                    ));
                }
                constraints.add(new IndexColumnConstraint<>("fulltext", properties));
            } else if (ArrayType.unnest(ref.valueType()).equals(DataTypes.GEO_SHAPE)) {
                GeoReference geoReference;
                if (ref instanceof GeneratedReference genRef) {
                    geoReference = (GeoReference) genRef.reference();
                } else {
                    geoReference = (GeoReference) ref;
                }
                Map<String, Expression> properties = new HashMap<>();
                if (geoReference.distanceErrorPct() != null) {
                    properties.put(
                        "distance_error_pct",
                        Literal.fromObject(geoReference.distanceErrorPct())
                    );
                }
                if (geoReference.precision() != null) {
                    properties.put("precision", Literal.fromObject(geoReference.precision()));
                }
                if (geoReference.treeLevels() != null) {
                    properties.put("tree_levels", Literal.fromObject(geoReference.treeLevels()));
                }
                constraints.add(new IndexColumnConstraint<>(geoReference.geoTree(), new GenericProperties<>(properties)));
            }

            StorageSupport<?> storageSupport = ref.valueType().storageSupportSafe();
            boolean hasDocValuesPerDefault = storageSupport.getComputedDocValuesDefault(ref.indexType());
            if (hasDocValuesPerDefault != ref.hasDocValues()) {
                GenericProperties<Expression> properties = new GenericProperties<>(Map.of(
                    TableElementsAnalyzer.COLUMN_STORE_PROPERTY, Literal.fromObject(ref.hasDocValues())
                ));
                constraints.add(new ColumnStorageDefinition<>(properties));
            }

            String columnName = ident.leafName();
            elements.add(new ColumnDefinition<>(
                columnName,
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
            return new PrimaryKeyConstraint<>(tableInfo.pkConstraintName(), expressionsFromColumns(tableInfo.primaryKey()));
        }
        return null;
    }

    private List<IndexDefinition<Expression>> extractIndexDefinitions() {
        if (!(tableInfo instanceof DocTableInfo docTable)) {
            return List.of();
        }
        Collection<IndexReference> indexColumns = docTable.indexColumns();
        List<IndexDefinition<Expression>> elements = new ArrayList<>(indexColumns.size());
        for (var indexRef : indexColumns) {
            String name = indexRef.column().name();
            List<Expression> columns = expressionsFromReferences(indexRef.columns());
            if (indexRef.indexType().equals(IndexType.FULLTEXT)) {
                String analyzer = indexRef.analyzer();
                GenericProperties<Expression> properties;
                if (analyzer == null) {
                    properties = GenericProperties.empty();
                } else {
                    properties = new GenericProperties<>(Map.of(
                        FulltextAnalyzerResolver.CustomType.ANALYZER.getName(),
                        new StringLiteral(analyzer))
                    );
                }
                elements.add(new IndexDefinition<>(name, "fulltext", columns, properties));
            } else if (indexRef.indexType().equals(IndexType.PLAIN)) {
                elements.add(new IndexDefinition<>(name, "plain", columns, GenericProperties.empty()));
            }
        }
        return elements;
    }

    private Optional<PartitionedBy<Expression>> createPartitionedBy(DocTableInfo docTable) {
        if (docTable.partitionedBy().isEmpty()) {
            return Optional.empty();
        } else {
            return Optional.of(new PartitionedBy<>(expressionsFromColumns(docTable.partitionedBy())));
        }
    }

    private Optional<ClusteredBy<Expression>> createClusteredBy(DocTableInfo docTable) {
        ColumnIdent clusteredByColumn = docTable.clusteredBy();
        Expression clusteredBy = clusteredByColumn == null || clusteredByColumn.isSystemColumn()
            ? null
            : clusteredByColumn.toExpression();
        Expression numShards = new LongLiteral(docTable.numberOfShards());
        return Optional.of(new ClusteredBy<>(Optional.ofNullable(clusteredBy), Optional.of(numShards)));
    }

    private GenericProperties<Expression> extractTableProperties() {
        // WITH ( key = value, ... )
        Map<String, Expression> properties = new HashMap<>();
        if (tableInfo instanceof DocTableInfo docTable) {
            Expression numReplicas = new StringLiteral(docTable.numberOfReplicas());
            properties.put(TableParameters.stripIndexPrefix(NumberOfReplicas.SETTING.getKey()), numReplicas);
            properties.put("column_policy", new StringLiteral(docTable.columnPolicy().lowerCaseName()));
        }
        Map<String, Object> tableParameters = TableParameters.tableParametersFromIndexMetadata(tableInfo.parameters());
        for (Map.Entry<String, Object> entry : tableParameters.entrySet()) {
            properties.put(
                TableParameters.stripIndexPrefix(entry.getKey()),
                Literal.fromObject(entry.getValue())
            );
        }
        return new GenericProperties<>(properties);
    }

    private List<Expression> expressionsFromReferences(List<Reference> columns) {
        List<Expression> expressions = new ArrayList<>(columns.size());
        for (Reference ident : columns) {
            expressions.add(ident.column().toExpression());
        }
        return expressions;
    }

    private List<Expression> expressionsFromColumns(List<ColumnIdent> columns) {
        List<Expression> expressions = new ArrayList<>(columns.size());
        for (ColumnIdent ident : columns) {
            expressions.add(ident.toExpression());
        }
        return expressions;
    }

}
