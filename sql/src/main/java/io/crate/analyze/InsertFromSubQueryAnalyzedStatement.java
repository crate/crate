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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.Path;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Field;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class InsertFromSubQueryAnalyzedStatement extends AbstractInsertAnalyzedStatement implements AnalyzedRelation {

    private AnalyzedRelation subQueryRelation;
    @Nullable
    private Map<Reference, Symbol> onDuplicateKeyAssignments;

    public InsertFromSubQueryAnalyzedStatement(AnalyzedRelation subQueryRelation, TableInfo targetTableInfo) {
        tableInfo(targetTableInfo);
        this.subQueryRelation = subQueryRelation;
    }

    public AnalyzedRelation subQueryRelation() {
        return this.subQueryRelation;
    }

    public void subQueryRelation(AnalyzedRelation subQueryRelation) {
        this.subQueryRelation = subQueryRelation;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitInsertFromSubQueryStatement(this, context);
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

    public void onDuplicateKeyAssignments(Map<Reference, Symbol> assignments) {
        onDuplicateKeyAssignments = assignments;
    }

    @Nullable
    public Map<Reference, Symbol> onDuplicateKeyAssignments() {
        return onDuplicateKeyAssignments;
    }

}
