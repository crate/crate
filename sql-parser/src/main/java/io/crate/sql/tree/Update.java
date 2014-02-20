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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;
import java.util.List;

public class Update extends Statement {

    private final Table table;
    private final List<Assignment> assignments;
    private final Optional<Expression> where;

    public Update(Table table, List<Assignment> assignments, @Nullable Expression where) {
        Preconditions.checkNotNull(table, "table is null");
        Preconditions.checkNotNull(assignments, "assignments are null");
        this.table = table;
        this.assignments = assignments;
        this.where = Optional.fromNullable(where);
    }

    public Table table() {
        return table;
    }

    public List<Assignment> assignements() {
        return assignments;
    }

    public Optional<Expression> whereClause() {
        return where;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, assignments, where);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("assignments", assignments)
                .add("where", where.orNull())
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Update update = (Update) o;

        if (!assignments.equals(update.assignments)) return false;
        if (!table.equals(update.table)) return false;
        if (where != null ? !where.equals(update.where) : update.where != null) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitUpdate(this, context);
    }
}
