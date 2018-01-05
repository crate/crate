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

import javax.annotation.Nullable;
import java.util.List;

public class Update extends Statement {

    private final Relation relation;
    private final List<Assignment> assignments;
    private final Expression where;

    public Update(Relation relation, List<Assignment> assignments, @Nullable Expression where) {
        Preconditions.checkNotNull(relation, "relation is null");
        Preconditions.checkNotNull(assignments, "assignments are null");
        this.relation = relation;
        this.assignments = assignments;
        this.where = where;
    }

    public Relation relation() {
        return relation;
    }

    public List<Assignment> assignements() {
        return assignments;
    }

    @Nullable
    public Expression whereClause() {
        return where;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(relation, assignments, where);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("relation", relation)
            .add("assignments", assignments)
            .add("where", where)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Update update = (Update) o;

        if (!assignments.equals(update.assignments)) return false;
        if (!relation.equals(update.relation)) return false;
        if (where != null ? !where.equals(update.where) : update.where != null) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdate(this, context);
    }
}
