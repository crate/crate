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
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ShowPartitions
        extends Statement
{
    private final QualifiedName table;
    private final Optional<Expression> where;
    private final List<SortItem> orderBy;
    private final Optional<Expression> limit;
    private final Optional<Expression> offset;

    public ShowPartitions(QualifiedName table,
                          Optional<Expression> where,
                          List<SortItem> orderBy,
                          Optional<Expression> limit,
                          Optional<Expression> offset)
    {
        this.table = checkNotNull(table, "table is null");
        this.where = checkNotNull(where, "where is null");
        this.orderBy = ImmutableList.copyOf(checkNotNull(orderBy, "orderBy is null"));
        this.limit = checkNotNull(limit, "limit is null");
        this.offset = checkNotNull(offset, "offset is null");
    }

    public QualifiedName getTable()
    {
        return table;
    }

    public Optional<Expression> getWhere()
    {
        return where;
    }

    public List<SortItem> getOrderBy()
    {
        return orderBy;
    }

    public Optional<Expression> getLimit()
    {
        return limit;
    }

    public Optional<Expression> getOffset()
    {
        return offset;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowPartitions(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(table, where, orderBy, limit, offset);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowPartitions o = (ShowPartitions) obj;
        return Objects.equal(table, o.table) &&
                Objects.equal(where, o.where) &&
                Objects.equal(orderBy, o.orderBy) &&
                Objects.equal(limit, o.limit) &&
                Objects.equal(offset, o.offset);
    }

    @Override
    public String toString()
    {
        return MoreObjects.toStringHelper(this)
                .add("table", table)
                .add("where", where)
                .add("orderBy", orderBy)
                .add("limit", limit)
                .add("offset", offset)
                .toString();
    }
}
