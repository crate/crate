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

package io.crate.analyze;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Reference;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public final class AnalyzedInsertStatement implements AnalyzedStatement {

    @Nullable
    private List<List<Symbol>> rows;
    @Nullable
    private AnalyzedStatement subRelation;

    private final Map<Reference, Symbol> onDuplicateKeyAssignments;

    private AnalyzedInsertStatement(Map<Reference, Symbol> onDuplicateKeyAssignments) {
        this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
    }

    AnalyzedInsertStatement(@Nullable List<List<Symbol>> rows,
                            Map<Reference, Symbol> onDuplicateKeyAssignments) {
        this(onDuplicateKeyAssignments);
        this.rows = rows;
    }

    AnalyzedInsertStatement(@Nullable AnalyzedStatement subRelation,
                            Map<Reference, Symbol> onDuplicateKeyAssignments) {
        this(onDuplicateKeyAssignments);
        this.subRelation = subRelation;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitInsert(this, context);
    }

    @Override
    public boolean isWriteOperation() {
        return true;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        if (rows != null) {
            for (List<Symbol> row : rows) {
                row.forEach(consumer);
            }
        } else if (subRelation != null) {
            subRelation.visitSymbols(consumer);
        }
        onDuplicateKeyAssignments.values().forEach(consumer);
    }

    @Nullable
    public List<List<Symbol>> rows() {
        return rows;
    }

    @Nullable
    public AnalyzedStatement subRelation() {
        return subRelation;
    }

    public Map<Reference, Symbol> onDuplicateKeyAssignments() {
        return onDuplicateKeyAssignments;
    }
}
