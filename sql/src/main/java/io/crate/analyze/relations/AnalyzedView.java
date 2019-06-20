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
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;

public final class AnalyzedView implements AnalyzedRelation {

    private final QualifiedName qualifiedName;
    private final RelationName name;
    private final String owner;
    private final AnalyzedRelation relation;
    private final Fields fields;
    private final List<Symbol> outputSymbols;

    public AnalyzedView(RelationName name, String owner, AnalyzedRelation relation) {
        this.name = name;
        this.qualifiedName = QualifiedName.of(name.schema(), name.name());
        this.owner = owner;
        this.fields = new Fields(relation.fields().size());
        this.relation = relation;
        for (Field field : relation.fields()) {
            fields.add(new Field(this, field.path(), field));
        }
        this.outputSymbols = List.copyOf(relation.fields());
    }

    public String owner() {
        return owner;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    public RelationName name() {
        return name;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitView(this, context);
    }

    @Override
    public Field getField(ColumnIdent path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on AnalyzedView is only supported for READ operations");
        }
        return fields.getWithSubscriptFallback(path, this, relation);
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
    public String toString() {
        return "AnalyzedView{" + "qualifiedName=" + qualifiedName +
               ", relation=" + relation +
               '}';
    }
}
