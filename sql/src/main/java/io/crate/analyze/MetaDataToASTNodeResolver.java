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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.sql.tree.*;
import io.crate.types.*;
import org.apache.lucene.analysis.*;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.analysis.CustomAnalyzer;

import java.util.*;

public class MetaDataToASTNodeResolver {


    public MetaDataToASTNodeResolver() {}

    public static CreateTable resolveCreateTable(TableInfo info) {
        Extractor extractor = new Extractor(info);
        return extractor.extractCreateTable();
    }

    private static class Extractor {

        private final TableInfo tableInfo;

        public Extractor(TableInfo tableInfo) {
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
                if (info.indexType().equals(ReferenceInfo.IndexType.NO)) {
                    constraints.add(IndexColumnConstraint.OFF);
                }
                ColumnDefinition column = new ColumnDefinition(columnName, columnType, constraints);
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
            Expression clusterColumn = null;
            if (tableInfo.clusteredBy() != null) {
                clusterColumn = expressionFromColumn(tableInfo.clusteredBy());
            }
            Expression numShards = new LongLiteral(Integer.toString(tableInfo.numberOfShards()));
            options.add(new ClusteredBy(clusterColumn, numShards));
            // PARTITIONED BY (...)
            if (!tableInfo.partitionedBy().isEmpty()) {
                options.add(new PartitionedBy(expressionsFromColumns(tableInfo.partitionedBy())));
            }
            return options;
        }

        private GenericProperties extractTableProperties() {
            GenericProperties properties = new GenericProperties();
            ImmutableMap<String, Object> tableParameters = tableInfo.tableParameters();
            for (Map.Entry<String, Object> entry : tableParameters.entrySet()) {
                properties.add(new GenericProperty(entry.getKey(), literalFromObject(entry.getValue())));
            }
            return properties;
        }

        private static Expression literalFromObject(Object value) {
            Expression expression = null;
            if (value == null) {
                expression = new NullLiteral();
            } else if (value instanceof String) {
                expression = new StringLiteral((String) value);
            } else if (value instanceof Number) {
                if (value instanceof Float || value instanceof Double) {
                    expression = new DoubleLiteral(value.toString());
                } else if (value instanceof Short || value instanceof Integer || value instanceof Long){
                    expression = new LongLiteral(value.toString());
                }
            } else if (value instanceof Boolean) {
                expression = new BooleanLiteral(value.toString());
            } else if (value instanceof Object[]) {
                List<Expression> expressions = new ArrayList<>();
                for (Object o : (Object[]) value) {
                    expressions.add(literalFromObject(o));
                }
                expression = new ArrayLiteral(expressions);
            } else if (value instanceof Map) {
                Multimap<String, Expression> map = HashMultimap.create();
                @SuppressWarnings("unchecked") Map<String, Object> valueMap = (Map<String, Object>) value;
                for (String key : valueMap.keySet()) {
                    map.put(key, literalFromObject(valueMap.get(key)));
                }
                expression = new ObjectLiteral(map);
            }
            return expression;
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
                fqn = new SubscriptExpression(fqn, literalFromObject(child));
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
