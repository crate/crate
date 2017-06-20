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

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class Query extends Statement {
    private final Optional<With> with;
    private final QueryBody queryBody;
    private final List<SortItem> orderBy;
    private final Optional<Expression> limit;
    private final Optional<Expression> offset;

    public Query(
        Optional<With> with,
        QueryBody queryBody,
        List<SortItem> orderBy,
        Optional<Expression> limit,
        Optional<Expression> offset) {
        checkNotNull(with, "with is null");
        checkNotNull(queryBody, "queryBody is null");
        checkNotNull(orderBy, "orderBy is null");
        checkNotNull(limit, "limit is null");
        checkNotNull(offset, "offset is null");

        this.with = with;
        this.queryBody = queryBody;
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
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("with", with)
            .add("queryBody", queryBody)
            .add("orderBy", orderBy)
            .add("limit", limit)
            .add("offset", offset)
            .omitNullValues()
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
        Query o = (Query) obj;
        return Objects.equal(with, o.with) &&
               Objects.equal(queryBody, o.queryBody) &&
               Objects.equal(orderBy, o.orderBy) &&
               Objects.equal(limit, o.limit) &&
               Objects.equal(offset, o.offset);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(with, queryBody, orderBy, limit, offset);
    }

    @Override
    public PrivilegeType privilegeType() {
        return PrivilegeType.DQL;
    }

    @Override
    public PrivilegeClazz privilegeClazz() {
        return PrivilegeClazz.TABLE;
    }

    @Override
    public String privilegeIdent() {
        return null;
    }
}
