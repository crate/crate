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
import com.google.common.base.Preconditions;

import java.util.Optional;

public class Delete extends Statement {

    private final Relation relation;
    private final Optional<Expression> where;

    public Delete(Relation relation, Optional<Expression> where) {
        Preconditions.checkNotNull(relation, "relation is null");
        this.relation = relation;
        this.where = where;
    }

    public Relation getRelation() {
        return relation;
    }

    public Optional<Expression> getWhere() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDelete(this, context);
    }


    @Override
    public int hashCode() {
        return Objects.hashCode(relation, where);
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("relation", relation)
            .add("where", where)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Delete delete = (Delete) o;

        if (relation != null ? !relation.equals(delete.relation) : delete.relation != null) return false;
        if (where != null ? !where.equals(delete.where) : delete.where != null) return false;

        return true;
    }

    @Override
    public PrivilegeType privilegeType() {
        return PrivilegeType.DML;
    }

    @Override
    public PrivilegeClazz privilegeClazz() {
        return PrivilegeClazz.TABLE;
    }

    @Override
    public String privilegeIdent() {
        return relation.toString();
    }
}
