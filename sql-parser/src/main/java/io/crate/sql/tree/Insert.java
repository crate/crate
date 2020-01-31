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

import java.util.Collections;
import java.util.List;

public final class Insert<T> extends Statement {

    private final Table<T> table;
    private final DuplicateKeyContext<T> duplicateKeyContext;
    private final List<String> columns;
    private final Query insertSource;
    private final List<SelectItem> returning;

    public Insert(Table<T> table,
                  Query insertSource,
                  List<String> columns,
                  List<SelectItem> returning,
                  DuplicateKeyContext<T> duplicateKeyContext) {
        this.table = table;
        this.columns = columns;
        this.insertSource = insertSource;
        this.duplicateKeyContext = duplicateKeyContext;
        this.returning = returning;
    }

    public Table table() {
        return table;
    }

    public List<String> columns() {
        return columns;
    }

    public Query insertSource() {
        return insertSource;
    }

    public DuplicateKeyContext<T> duplicateKeyContext() {
        return duplicateKeyContext;
    }

    public List<SelectItem> returningClause() {
        return returning;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, columns, insertSource, returning);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Insert<?> insert = (Insert<?>) o;
        return table.equals(insert.table) &&
               columns.equals(insert.columns) &&
               insertSource.equals(insert.insertSource) &&
               returning.equals(insert.returning);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("table", table)
            .add("columns", columns)
            .add("insertSource", insertSource)
            .add("returning", returning)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitInsert(this, context);
    }

    public static class DuplicateKeyContext<T> {

        private static final DuplicateKeyContext<?> NONE =
            new DuplicateKeyContext<>(Type.NONE, Collections.emptyList(), Collections.emptyList());

        public static <T> DuplicateKeyContext<T> none() {
            return (DuplicateKeyContext<T>) NONE;
        }

        public enum Type {
            ON_CONFLICT_DO_UPDATE_SET,
            ON_CONFLICT_DO_NOTHING,
            NONE
        }

        private final Type type;
        private final List<Assignment<T>> onDuplicateKeyAssignments;
        private final List<String> constraintColumns;

        public DuplicateKeyContext(Type type,
                                   List<Assignment<T>> onDuplicateKeyAssignments,
                                   List<String> constraintColumns) {
            this.type = type;
            this.onDuplicateKeyAssignments = onDuplicateKeyAssignments;
            this.constraintColumns = constraintColumns;
        }

        public Type getType() {
            return type;
        }

        public List<Assignment<T>> getAssignments() {
            return onDuplicateKeyAssignments;
        }

        public List<String> getConstraintColumns() {
            return constraintColumns;
        }
    }
}
