/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class QuerySpecification
    extends QueryBody {
    private final Select select;
    private final List<Relation> from;
    @Nullable
    private final Expression where;
    private final List<Expression> groupBy;
    @Nullable
    private final Expression having;
    private final List<SortItem> orderBy;
    @Nullable
    private final Expression limit;
    @Nullable
    private final Expression offset;

    public QuerySpecification(
        Select select,
        @Nullable List<Relation> from,
        @Nullable Expression where,
        List<Expression> groupBy,
        @Nullable Expression having,
        List<SortItem> orderBy,
        @Nullable Expression limit,
        @Nullable Expression offset) {
        checkNotNull(select, "select is null");
        checkNotNull(groupBy, "groupBy is null");
        checkNotNull(orderBy, "orderBy is null");

        this.select = select;
        this.from = from;
        this.where = where;
        this.groupBy = groupBy;
        this.having = having;
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    public Select getSelect() {
        return select;
    }

    public List<Relation> getFrom() {
        return from;
    }

    @Nullable
    public Expression getWhere() {
        return where;
    }

    public List<Expression> getGroupBy() {
        return groupBy;
    }

    @Nullable
    public Expression getHaving() {
        return having;
    }

    public List<SortItem> getOrderBy() {
        return orderBy;
    }

    @Nullable
    public Expression getLimit() {
        return limit;
    }

    @Nullable
    public Expression getOffset() {
        return offset;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQuerySpecification(this, context);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("select", select)
            .add("from", from)
            .add("where", where)
            .add("groupBy", groupBy)
            .add("having", having)
            .add("orderBy", orderBy)
            .add("limit", limit)
            .add("offset", offset)
            .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        QuerySpecification o = (QuerySpecification) obj;
        return Objects.equal(select, o.select) &&
               Objects.equal(from, o.from) &&
               Objects.equal(where, o.where) &&
               Objects.equal(groupBy, o.groupBy) &&
               Objects.equal(having, o.having) &&
               Objects.equal(orderBy, o.orderBy) &&
               Objects.equal(limit, o.limit) &&
               Objects.equal(offset, o.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(select, from, where, groupBy, having, orderBy, limit, offset);
    }
}
