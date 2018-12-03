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

import io.crate.analyze.HavingClause;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;


/**
 * Models the relation that wraps a SET operation (Union/Except/Intercept) or a VALUES definition
 * in the AST built by the Parser when an ORDER BY and/or LIMIT and/or OFFSET clause that applies to
 * the whole operation is defined. E.g.:
 *
 * <code>
 *     SELECT * FROM ([Query t1])
 *     UNION
 *     SELECT * FROM ([Query t2])
 *     ORDER BY 1
 * </code>
 *
 * It's represented like this:
 *
 *        OrderedLimitedRelation -> (ORDER BY 1)
 *                 |
 *                 |
 *            UnionSelect        -> (Never contains an OrderBy or Limit/Offset)
 *                /     \
 *               /       \
 * QueriedRelation_t1  QueriedRelation_t2
 */
public class OrderedLimitedRelation implements QueriedRelation {

    private final QueriedRelation childRelation;
    private final QuerySpec querySpec;

    public OrderedLimitedRelation(QueriedRelation childRelation,
                                  OrderBy orderBy,
                                  @Nullable Symbol limit,
                                  @Nullable Symbol offset) {
        this.childRelation = childRelation;
        this.querySpec = new QuerySpec();
        querySpec.limit(limit);
        querySpec.orderBy(orderBy);
        querySpec.offset(offset);
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitOrderedLimitedRelation(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        return childRelation.getField(path, operation);
    }

    @Override
    public List<Field> fields() {
        return childRelation.fields();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return childRelation.getQualifiedName();
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        childRelation.setQualifiedName(qualifiedName);
    }

    public QueriedRelation childRelation() {
        return childRelation;
    }

    @Override
    public QuerySpec querySpec() {
        return this.querySpec;
    }

    @Override
    public List<Symbol> outputs() {
        return childRelation.outputs();
    }

    @Override
    public WhereClause where() {
        return WhereClause.MATCH_ALL;
    }

    @Override
    public List<Symbol> groupBy() {
        return Collections.emptyList();
    }

    @Nullable
    @Override
    public HavingClause having() {
        return null;
    }

    public OrderBy orderBy() {
        return querySpec.orderBy();
    }

    @Nullable
    public Symbol limit() {
        return querySpec.limit();
    }

    @Nullable
    public Symbol offset() {
        return querySpec.offset();
    }

    @Override
    public boolean hasAggregates() {
        return false;
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }

    @Override
    public boolean isDistinct() {
        return false;
    }

    public OrderedLimitedRelation map(QueriedRelation newChild, Function<? super Symbol,? extends Symbol> mapper) {
        OrderBy orderBy = orderBy();
        Symbol limit = limit();
        Symbol offset = offset();
        return new OrderedLimitedRelation(
            newChild,
            orderBy == null ? null : orderBy.copyAndReplace(mapper),
            limit == null ? null : mapper.apply(limit),
            offset == null ? null : mapper.apply(offset)
        );
    }
}
