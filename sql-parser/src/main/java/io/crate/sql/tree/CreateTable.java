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
 * However, if you have executed any another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.sql.tree;

import com.google.common.base.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class CreateTable
        extends Statement
{
    private final QualifiedName name;
    private final Query query;

    public CreateTable(QualifiedName name, Query query)
    {
        this.name = checkNotNull(name, "name is null");
        this.query = checkNotNull(query, "query is null");
    }

    public QualifiedName getName()
    {
        return name;
    }

    public Query getQuery()
    {
        return query;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCreateTable(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name, query);
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
        CreateTable o = (CreateTable) obj;
        return Objects.equal(name, o.name)
                && Objects.equal(query, o.query);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("name", name)
                .add("query", query)
                .toString();
    }
}
