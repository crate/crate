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

import io.crate.analyze.relations.AbstractTableRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public final class QueriedTable<TR extends AbstractTableRelation> implements QueriedRelation {

    private final boolean isDistinct;
    private final TR tableRelation;
    private final QuerySpec querySpec;
    private final Fields fields;

    public QueriedTable(boolean isDistinct,
                        TR tableRelation,
                        Collection<? extends Path> outputNames,
                        QuerySpec querySpec) {
        this.isDistinct = isDistinct;
        this.tableRelation = tableRelation;
        this.querySpec = querySpec;
        this.fields = new Fields(outputNames.size());
        Iterator<Symbol> outputsIterator = querySpec.outputs().iterator();
        for (Path path : outputNames) {
            fields.add(path, new Field(this, path, outputsIterator.next().valueType()));
        }
    }

    public QueriedTable(boolean isDistinct, TR tableRelation, QuerySpec querySpec) {
        this(isDistinct, tableRelation, Relations.namesFromOutputs(querySpec.outputs()), querySpec);
    }

    public QuerySpec querySpec() {
        return querySpec;
    }

    @Override
    public boolean isDistinct() {
        return isDistinct;
    }

    public TR tableRelation() {
        return tableRelation;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedTable(this, context);
    }

    @Nullable
    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on SelectAnalyzedStatement is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return tableRelation.getQualifiedName();
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        tableRelation.setQualifiedName(qualifiedName);
    }

    @Override
    public String toString() {
        return "QueriedTable{" + tableRelation + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        QueriedTable<?> that = (QueriedTable<?>) o;

        return tableRelation.equals(that.tableRelation);
    }

    @Override
    public int hashCode() {
        return tableRelation.hashCode();
    }
}
