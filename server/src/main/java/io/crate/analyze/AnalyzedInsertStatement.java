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

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.common.collections.Maps;
import io.crate.execution.dsl.projection.builder.InputColumns;
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.doc.DocTableInfo;


public class AnalyzedInsertStatement implements AnalyzedStatement {

    private final DocTableInfo targetTable;
    private final AnalyzedRelation subQueryRelation;
    private final boolean ignoreDuplicateKeys;
    private final Map<Reference, Symbol> onDuplicateKeyAssignments;
    private final List<Reference> targetColumns;
    private final List<Symbol> primaryKeySymbols;
    private final List<Symbol> partitionedBySymbols;
    @Nullable
    private final Symbol clusteredBySymbol;

    /**
     * List of values or expressions used to be retrieved from the updated rows.
     */
    @Nullable
    private final List<Symbol> returnValues;

    AnalyzedInsertStatement(AnalyzedRelation subQueryRelation,
                            DocTableInfo tableInfo,
                            List<Reference> targetColumns,
                            boolean ignoreDuplicateKeys,
                            Map<Reference, Symbol> onDuplicateKeyAssignments,
                            @Nullable List<Symbol> returnValues) {
        this.targetTable = tableInfo;
        this.subQueryRelation = subQueryRelation;
        this.ignoreDuplicateKeys = ignoreDuplicateKeys;
        this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
        this.targetColumns = targetColumns;
        Map<ColumnIdent, Integer> columnPositions = toPositionMap(targetColumns);

        int clusteredByIdx = Objects.requireNonNullElse(columnPositions.get(tableInfo.clusteredBy()), -1);
        if (clusteredByIdx > -1) {
            clusteredBySymbol = new InputColumn(clusteredByIdx, targetColumns.get(clusteredByIdx).valueType());
        } else {
            clusteredBySymbol = null;
        }
        Map<ColumnIdent, GeneratedReference> generatedColumns =
            Maps.uniqueIndex(tableInfo.generatedColumns(), Reference::column);

        Map<ColumnIdent, Reference> defaultExpressionColumns =
            Maps.uniqueIndex(tableInfo.defaultExpressionColumns(), Reference::column);

        if (tableInfo.hasAutoGeneratedPrimaryKey()) {
            this.primaryKeySymbols = Collections.emptyList();
        } else {
            this.primaryKeySymbols = symbolsFromTargetColumnPositionOrGeneratedExpression(
                columnPositions,
                targetColumns,
                tableInfo.primaryKey(),
                generatedColumns,
                defaultExpressionColumns,
                true);
        }
        this.partitionedBySymbols = symbolsFromTargetColumnPositionOrGeneratedExpression(
            columnPositions,
            targetColumns,
            tableInfo.partitionedBy(),
            generatedColumns,
            defaultExpressionColumns,
            false);
        this.returnValues = returnValues;
    }

    private static Map<ColumnIdent, Integer> toPositionMap(List<Reference> targetColumns) {
        if (targetColumns.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<ColumnIdent, Integer> columnPositions = HashMap.newHashMap(targetColumns.size());
        ListIterator<Reference> it = targetColumns.listIterator();
        while (it.hasNext()) {
            columnPositions.put(it.next().column(), it.previousIndex());
        }
        return columnPositions;
    }

    private List<Symbol> symbolsFromTargetColumnPositionOrGeneratedExpression(Map<ColumnIdent, Integer> targetColumnMap,
                                                                              List<Reference> targetColumns,
                                                                              List<ColumnIdent> columns,
                                                                              Map<ColumnIdent, GeneratedReference> generatedColumns,
                                                                              Map<ColumnIdent, Reference> defaultExpressionColumns,
                                                                              boolean alwaysRequireColumn) {
        if (columns.isEmpty()) {
            return Collections.emptyList();
        }
        List<Symbol> symbols = new ArrayList<>(columns.size());
        InputColumns.SourceSymbols sourceSymbols = new InputColumns.SourceSymbols(targetColumns);
        for (ColumnIdent column : columns) {
            Integer position = targetColumnMap.get(column);
            if (position == null) {
                final Symbol symbol;
                var reference = defaultExpressionColumns.get(column);
                if (reference != null) {
                    symbol = InputColumns.create(
                        requireNonNull(
                            reference.defaultExpression(),
                            "Column " + column + " must contain a default expression"),
                        sourceSymbols);
                } else {
                    GeneratedReference generatedRef = generatedColumns.get(column);
                    if (generatedRef == null) {
                        Reference columnRef = requireNonNull(targetTable.getReference(column),
                                                             "Column " + column + " must exist in table " +
                                                             targetTable.ident());
                        symbol = InputColumns.create(columnRef, sourceSymbols);
                    } else {
                        symbol = InputColumns.create(generatedRef.generatedExpression(), sourceSymbols);
                    }
                }

                if (symbol.any(Symbol.IS_COLUMN)) {
                    if (alwaysRequireColumn) {
                        throw new IllegalArgumentException(String.format(
                            Locale.ENGLISH,
                            "Column \"%s\" is required but is missing from the insert statement", column.sqlFqn()));
                    } else {
                        symbols.add(Literal.NULL);
                    }
                } else {
                    symbols.add(symbol);
                }


            } else {
                symbols.add(new InputColumn(position, targetColumns.get(position).valueType()));
            }
        }
        return symbols;
    }

    @Nullable
    @Override
    public List<Symbol> outputs() {
        return returnValues;
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
        return analyzedStatementVisitor.visitAnalyzedInsertStatement(this, context);
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        Relations.traverseDeepSymbols(subQueryRelation, consumer);
        targetColumns.forEach(consumer);
        onDuplicateKeyAssignments.values().forEach(consumer);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    public boolean isIgnoreDuplicateKeys() {
        return ignoreDuplicateKeys;
    }

    public Map<Reference, Symbol> onDuplicateKeyAssignments() {
        return onDuplicateKeyAssignments;
    }

    public List<Symbol> primaryKeySymbols() {
        return primaryKeySymbols;
    }

    public List<Symbol> partitionedBySymbols() {
        return partitionedBySymbols;
    }

    @Nullable
    public Symbol clusteredBySymbol() {
        return clusteredBySymbol;
    }
}
