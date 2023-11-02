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

import java.util.Objects;

public class AlterTableRenameColumn<T> extends Statement {

    private final Table<T> table;
    private final Expression column;
    private final Expression newName;

    public AlterTableRenameColumn(Table<T> table, Expression column, Expression newName) {
        this.table = table;
        this.column = column;
        this.newName = newName;
    }

    public Table<T> table() {
        return table;
    }

    public Expression column() {
        return column;
    }

    public Expression newName() {
        return newName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlterTableRenameColumn<?> that = (AlterTableRenameColumn<?>) o;
        return table.equals(that.table) && column.equals(that.column) && newName.equals(that.newName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, column, newName);
    }

    @Override
    public String toString() {
        return "AlterTableRenameColumn{" +
            "table=" + table +
            ", column=" + column +
            ", newName=" + newName +
            '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableRenameColumnStatement(this, context);
    }
}
