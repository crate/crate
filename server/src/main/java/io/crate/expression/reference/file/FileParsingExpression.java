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

package io.crate.expression.reference.file;

import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class FileParsingExpression implements CollectExpression<Row, Object> {

    protected final RowContext rowContext;
    protected Row row;

    private final RelationName tableIdent;
    private final List<Reference> targetColumns;

    public FileParsingExpression(RelationName tableIdent, List<Reference> targetColumns, RowContext rowContext) {
        this.tableIdent = tableIdent;
        this.targetColumns = targetColumns;
        this.rowContext = rowContext;
    }

    @Override
    public void setNextRow(Row row) {
        this.row = row;
        // Expressions must reset previous row's context.
        // Context will be updated on the first value call
        // and will be re-used in the other expression computations, taking the same Row as data source.
        this.rowContext.rowAsMap = null;
        this.rowContext.newColumns.clear();

    }

    /**
     * Adjusts plan if first line/header has unknown columns. Called only once.
     *
     * <ul>
     *  <li>Adds new unknown columns</li>
     *  <li>Removes generated columns from targets if column is not provided. Indexer will inject computed value itself.
     *  </li>
     * </ul>
     */
    protected void finalizeTargetColumns(Iterable<String> allColumns) {
        for (String columnName: allColumns) {
            // Method is called once so it's fine to do heavy calculation.
            // TODO: Introduce smth like Map<Ident, Reference> once non-homogenous JSON support is added
            // and update it in sync with projection targets update to do fast check.
            Reference existingRef = targetColumns
                .stream()
                .filter(ref -> ref.column().sqlFqn().equals(columnName))
                .findFirst().orElse(null);

            if (existingRef == null) {
                rowContext.newColumns.add(new DynamicReference(new ReferenceIdent(tableIdent, columnName), RowGranularity.DOC, 0));
            }
        }

    }

    abstract Map<String, Object> parseRow();

    public static class RowContext {
        private Map<String, Object> rowAsMap = null;
        private final List<Reference> newColumns = new ArrayList<>();

        public void rowAsMap(@Nullable Map<String, Object> rowAsMap) {
            this.rowAsMap = rowAsMap;
        }

        @Nullable
        public Map<String, Object> rowAsMap() {
            return rowAsMap;
        }

        @NotNull
        public List<Reference> newColumns() {
            return newColumns;
        }
    }


}
