/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.relations;

import io.crate.analyze.Fields;
import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AliasedAnalyzedRelation implements AnalyzedRelation {

    private final AnalyzedRelation relation;
    private final QualifiedName qualifiedName;
    private final List<String> columnAliases;
    private final Fields fields;
    private final List<Symbol> outputSymbols;
    private final Map<ColumnIdent, ColumnIdent> aliasToColumnMapping;

    public AliasedAnalyzedRelation(AnalyzedRelation relation, QualifiedName relationAlias) {
        this(relation, relationAlias, List.of());
    }

    AliasedAnalyzedRelation(AnalyzedRelation relation, QualifiedName relationAlias, List<String> columnAliases) {
        this.relation = relation;
        qualifiedName = relationAlias;
        this.columnAliases = columnAliases;
        List<Field> originalFields = relation.fields();
        fields = new Fields(originalFields.size());
        outputSymbols = List.copyOf(relation.fields());
        aliasToColumnMapping = new HashMap<>(columnAliases.size());
        createAliasToColumnMappingAndNewFields(columnAliases, originalFields);
    }

    private void createAliasToColumnMappingAndNewFields(List<String> columnAliases, List<Field> originalFields) {
        for (int i = 0; i < originalFields.size(); i++) {
            Field field = originalFields.get(i);
            ColumnIdent ci = field.path();
            if (i < columnAliases.size()) {
                String columnAlias = columnAliases.get(i);
                if (columnAlias != null) {
                    aliasToColumnMapping.put(new ColumnIdent(columnAlias), ci);
                    ci = new ColumnIdent(columnAlias);
                }
            }
            fields.add(new Field(this, ci, field));
        }
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    List<String> columnAliases() {
        return columnAliases;
    }

    @Override
    public Field getField(ColumnIdent path,
                          Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        Field field = fields.get(path);
        if (field == null) {
            ColumnIdent childPath = aliasToColumnMapping.getOrDefault(path, path);
            Field originalField = relation.getField(childPath, operation);
            if (originalField != null) {
                field = new Field(this, path, originalField);
            }
        }
        return field;
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return qualifiedName;
    }

    @Override
    public List<Symbol> outputs() {
        return outputSymbols;
    }

    @Override
    public WhereClause where() {
        return WhereClause.MATCH_ALL;
    }

    @Override
    public List<Symbol> groupBy() {
        return List.of();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return null;
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return null;
    }

    @Nullable
    @Override
    public Symbol limit() {
        return null;
    }

    @Nullable
    @Override
    public Symbol offset() {
        return null;
    }

    @Override
    public boolean hasAggregates() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public String toString() {
        return relation + " AS " + qualifiedName;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitAliasedAnalyzedRelation(this, context);
    }
}
