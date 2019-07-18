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
import io.crate.expression.symbol.InputColumn;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;

public final class ValuesRelation implements AnalyzedRelation {

    private static final QualifiedName VALUES = new QualifiedName("VALUES");
    private final List<List<Symbol>> rows;
    private final Fields fields;
    private final List<Symbol> outputs;

    public ValuesRelation(List<List<Symbol>> rows) {
        assert !rows.isEmpty()
            : "Parser restricts VALUES syntax so that at least 1 row must be present. VALUES relation cannot contain empty rows.";
        List<Symbol> firstRow = rows.get(0);
        this.rows = rows;
        this.fields = new Fields(firstRow.size());
        this.outputs = InputColumn.mapToInputColumns(firstRow);
        for (int i = 0; i < firstRow.size(); i++) {
            Symbol column = firstRow.get(i);
            fields.add(new Field(this, Symbols.pathFromSymbol(column), outputs.get(i)));
        }
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitValues(this, context);
    }

    @Override
    public Field getField(ColumnIdent path,
                          Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on QueriedSelectRelation is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return VALUES;
    }

    @Override
    public List<Symbol> outputs() {
        return outputs;
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

    public List<List<Symbol>> rows() {
        return rows;
    }

    @Override
    public void visitSymbols(Consumer<? super Symbol> consumer) {
        for (List<Symbol> row : rows) {
            for (Symbol column : row) {
                consumer.accept(column);
            }
        }
    }
}
