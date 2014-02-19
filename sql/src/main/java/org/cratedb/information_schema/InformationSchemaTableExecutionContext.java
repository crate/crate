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

package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.collect.*;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.lucene.LuceneFieldMapper;
import org.cratedb.lucene.fields.LuceneField;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.TableUnknownException;
import org.cratedb.sql.parser.parser.NodeType;
import org.cratedb.sql.parser.parser.ValueNode;
import org.cratedb.sql.types.SQLFieldMapper;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class InformationSchemaTableExecutionContext implements ITableExecutionContext {

    public static final String SCHEMA_NAME = "INFORMATION_SCHEMA";
    private final String tableName;

    private final ImmutableMap<String, InformationSchemaTable> tablesMap;
    private final ImmutableMap<String, ColumnDefinition> columnDefinitions;

    @Inject
    public InformationSchemaTableExecutionContext(Map<String,
                InformationSchemaTable> informationSchemaTables, @Assisted String tableName) {
        this.tablesMap = ImmutableMap.copyOf(informationSchemaTables);
        if (!this.tablesMap.containsKey(tableName)) {
            throw new TableUnknownException(tableName);
        }
        this.tableName = tableName;
        ImmutableMap.Builder<String, ColumnDefinition> builder = new ImmutableMap.Builder<>();
        int position = 0;
        for (Map.Entry<String, LuceneField> entry: this.tablesMap.get(this.tableName).fieldMapper()
                .entrySet()) {
            builder.put(entry.getKey(), entry.getValue().getColumnDefinition(tableName, position++));
        }
        this.columnDefinitions = builder.build();
    }

    @Override
    public LuceneFieldMapper luceneFieldMapper() {
        return tablesMap.get(tableName).fieldMapper();
    }

    @Override
    public SQLFieldMapper mapper() {
        throw new UnsupportedOperationException("Information Schema currently has no " +
                "SQLFieldMapper");
    }

    @Override
    public Object mappedValue(String name, Object value) {
        return value;
    }

    @Override
    public List<String> primaryKeys() {
        return new ArrayList<>(0);
    }

    @Override
    public List<String> primaryKeysIncludingDefault() {
        return new ArrayList<>(0);
    }

    @Override
    public Iterable<String> allCols() {
        return tablesMap.get(tableName).cols();
    }

    @Override
    public boolean hasCol(String name) {
        return tablesMap.get(tableName).fieldMapper().containsKey(name);
    }

    @Override
    public ColumnDefinition getColumnDefinition(String name) {
        return columnDefinitions.get(name);
    }

    @Override
    public Boolean isRouting(String name) {
        return false;
    }

    public boolean tableIsAlias() {
        return false;
    }

    @Override
    public boolean isMultiValued(String columnName) {
        LuceneField field = luceneFieldMapper().get(columnName);
        return field != null && field.allowMultipleValues;
    }

    @Override
    public Expression getCollectorExpression(ValueNode node) {

        if (node.getNodeType()!= NodeType.COLUMN_REFERENCE &&
                node.getNodeType() != NodeType.NESTED_COLUMN_REFERENCE){
            return null;
        }

        ColumnDefinition columnDefinition = getColumnDefinition(node.getColumnName());
        if (columnDefinition == null) {
            throw new SQLParseException(String.format("Unknown column '%s'", node.getColumnName()));
        }
        return FieldLookupExpression.create(columnDefinition);
   }
}