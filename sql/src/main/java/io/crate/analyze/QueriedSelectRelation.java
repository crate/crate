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

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.collect.Lists.transform;

public class QueriedSelectRelation implements AnalyzedRelation {

    private final Fields fields;
    private final QuerySpec querySpec;
    private final boolean isDistinct;
    private final AnalyzedRelation subRelation;

    public QueriedSelectRelation(boolean isDistinct,
                                 AnalyzedRelation subRelation,
                                 Collection<? extends ColumnIdent> outputNames,
                                 QuerySpec querySpec) {
        this.isDistinct = isDistinct;
        this.subRelation = subRelation;
        this.querySpec = querySpec;
        this.fields = new Fields(outputNames.size());
        Iterator<Symbol> outputsIterator = querySpec.outputs().iterator();
        for (ColumnIdent path : outputNames) {
            fields.add(new Field(this, path, outputsIterator.next()));
        }
    }

    public AnalyzedRelation subRelation() {
        return subRelation;
    }

    @Override
    public boolean isDistinct() {
        return isDistinct;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitQueriedSelectRelation(this, context);
    }

    @Nullable
    @Override
    public Field getField(ColumnIdent path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on QueriedSelectRelation is only supported for READ operations");
        }
        return fields.getWithSubscriptFallback(path, this, subRelation);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return subRelation.getQualifiedName();
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        subRelation.setQualifiedName(qualifiedName);
    }

    @Override
    public List<Symbol> outputs() {
        return querySpec.outputs();
    }

    @Override
    public WhereClause where() {
        return querySpec.where();
    }

    @Override
    public List<Symbol> groupBy() {
        return querySpec.groupBy();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return querySpec.having();
    }

    @Nullable
    @Override
    public OrderBy orderBy() {
        return querySpec.orderBy();
    }

    @Nullable
    @Override
    public Symbol limit() {
        return querySpec.limit();
    }

    @Nullable
    @Override
    public Symbol offset() {
        return querySpec.offset();
    }

    @Override
    public boolean hasAggregates() {
        return querySpec.hasAggregates();
    }

    /**
     * Creates a new relation with the newSubrelation as child.
     * Fields will be re-mapped (They contain a hard-reference to a relation),
     * but for this to work the new relation must have semantically equal outputs to the old relation.
     */
    public AnalyzedRelation replaceSubRelation(AnalyzedRelation newSubRelation) {
        var mapFieldsToNewRelation = FieldReplacer.bind(
            f -> {
                if (f.relation().equals(subRelation)) {
                    int idx = subRelation.fields().indexOf(f);
                    if (idx >= 0) {
                        return newSubRelation.fields().get(idx);
                    }
                }
                return f;
            }
        );
        return new QueriedSelectRelation(
            isDistinct,
            newSubRelation,
            transform(fields.asList(), Field::path),
            querySpec.map(mapFieldsToNewRelation)
        );
    }
}
