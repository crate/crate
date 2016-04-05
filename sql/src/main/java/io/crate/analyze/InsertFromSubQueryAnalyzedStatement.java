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

import com.carrotsearch.hppc.ObjectIntMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.InputColumn;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.IntMaps;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReferenceInfo;
import io.crate.metadata.Path;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.projection.builder.InputCreatingVisitor;

import javax.annotation.Nullable;
import java.util.*;


public class InsertFromSubQueryAnalyzedStatement implements AnalyzedRelation, AnalyzedStatement {

    private final DocTableInfo targetTable;
    private final AnalyzedRelation subQueryRelation;

    @Nullable
    private final Map<Reference, Symbol> onDuplicateKeyAssignments;
    private final List<Reference> targetColumns;
    private final List<Symbol> primaryKeySymbols;
    private final List<Symbol> partitionedBySymbols;
    private final int clusteredByIdx;

    public InsertFromSubQueryAnalyzedStatement(AnalyzedRelation subQueryRelation,
                                               DocTableInfo tableInfo,
                                               List<Reference> targetColumns,
                                               @Nullable Map<Reference, Symbol> onDuplicateKeyAssignments) {
        this.targetTable = tableInfo;
        this.subQueryRelation = subQueryRelation;
        this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
        this.targetColumns = targetColumns;
        ObjectIntMap<ColumnIdent> targetColumnMap = IntMaps.uniqueIndex(targetColumns, Reference.TO_COLUMN_IDENT);
        clusteredByIdx = targetColumnMap.getOrDefault(tableInfo.clusteredBy(), -1);
        ImmutableMap<ColumnIdent, GeneratedReferenceInfo> generatedColumns =
                Maps.uniqueIndex(tableInfo.generatedColumns(), ReferenceInfo.TO_COLUMN_IDENT);

        if (tableInfo.hasAutoGeneratedPrimaryKey()) {
            this.primaryKeySymbols = Collections.emptyList();
        } else {
            this.primaryKeySymbols = symbolsFromTargetColumnPositionOrGeneratedExpression(
                    targetColumnMap, targetColumns, tableInfo.primaryKey(), generatedColumns);
        }
        this.partitionedBySymbols = symbolsFromTargetColumnPositionOrGeneratedExpression(
                targetColumnMap, targetColumns, tableInfo.partitionedBy(), generatedColumns);
    }

    private static List<Symbol> symbolsFromTargetColumnPositionOrGeneratedExpression(ObjectIntMap<ColumnIdent> targetColumnMap,
                                                                                     List<Reference> targetColumns,
                                                                                     List<ColumnIdent> columns,
                                                                                     Map<ColumnIdent, GeneratedReferenceInfo> generatedColumns) {
        if (columns.isEmpty()) {
            return Collections.emptyList();
        }

        List<Symbol> symbols = new ArrayList<>(columns.size());
        InputCreatingVisitor.Context inputContext = null;
        for (ColumnIdent column : columns) {
            if (targetColumnMap.containsKey(column)) {
                int colPosition = targetColumnMap.get(column);
                symbols.add(new InputColumn(colPosition, targetColumns.get(colPosition).valueType()));
            } else {
                GeneratedReferenceInfo generatedReferenceInfo = generatedColumns.get(column);
                if (generatedReferenceInfo == null) {
                    throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                            "Column \"%s\" is required but is missing from the insert statement", column.sqlFqn()));
                }

                if (inputContext == null) {
                    inputContext = new InputCreatingVisitor.Context(targetColumns);
                }
                Symbol symbol = InputCreatingVisitor.INSTANCE.process(generatedReferenceInfo.generatedExpression(), inputContext);
                symbols.add(symbol);
            }
        }
        return symbols;
    }

    public List<Reference> columns() {
        return targetColumns;
    }

    public DocTableInfo tableInfo() {
        return targetTable;
    }

    public AnalyzedRelation subQueryRelation() {
        return this.subQueryRelation;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitInsertFromSubQueryStatement(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitInsertFromQuery(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path) {
        return null;
    }

    @Nullable
    @Override
    public Field getWritableField(Path path) throws UnsupportedOperationException, ColumnUnknownException {
        return null;
    }

    @Override
    public List<Field> fields() {
        return ImmutableList.of();
    }

    @Nullable
    public Map<Reference, Symbol> onDuplicateKeyAssignments() {
        return onDuplicateKeyAssignments;
    }

    public List<Symbol> primaryKeySymbols() {
        return primaryKeySymbols;
    }

    public List<Symbol> partitionedBySymbols() {
        return partitionedBySymbols;
    }

    public int clusteredByIdx() {
        return clusteredByIdx;
    }
}
