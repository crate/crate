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

package io.crate.sql.tree;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class Query extends Statement {

    private final Optional<With> with;
    private final QueryBody queryBody;
    private final List<SortItem> orderBy;
    private final Optional<Expression> limit;
    private final Optional<Expression> offset;

    public Query(Optional<With> with,
                 QueryBody queryBody,
                 List<SortItem> orderBy,
                 Optional<Expression> limit,
                 Optional<Expression> offset) {
        this.with = requireNonNull(with, "with is null");
        this.queryBody = requireNonNull(queryBody, "queryBody is null");
        this.orderBy = orderBy;
        this.limit = limit;
        this.offset = offset;
    }

    public Optional<With> getWith() {
        return with;
    }

    public QueryBody getQueryBody() {
        return queryBody;
    }

    public List<SortItem> getOrderBy() {
        return orderBy;
    }

    public Optional<Expression> getLimit() {
        return limit;
    }

    public Optional<Expression> getOffset() {
        return offset;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitQuery(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Query query = (Query) o;
        return Objects.equals(with, query.with) &&
               Objects.equals(queryBody, query.queryBody) &&
               Objects.equals(orderBy, query.orderBy) &&
               Objects.equals(limit, query.limit) &&
               Objects.equals(offset, query.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hash(with, queryBody, orderBy, limit, offset);
    }

    @Override
    public String toString() {
        return "Query{" +
               "with=" + with +
               "queryBody=" + queryBody +
               ", orderBy=" + orderBy +
               ", limit=" + limit +
               ", offset=" + offset +
               '}';
    }
}
