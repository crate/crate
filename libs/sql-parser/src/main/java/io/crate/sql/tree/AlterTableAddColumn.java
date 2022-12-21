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

import java.util.List;
import java.util.Objects;

public class AlterTableAddColumn<T> extends Statement {

    private final Table<T> table;
    private final List<AddColumnDefinition<T>> addColumnDefinitions;

    public AlterTableAddColumn(Table<T> table, List<AddColumnDefinition<T>> addColumnDefinitions) {
        this.table = table;
        this.addColumnDefinitions = addColumnDefinitions;
    }

    public List<AddColumnDefinition<T>> tableElements() {
        return addColumnDefinitions;
    }

    public Table<T> table() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AlterTableAddColumn<?> that = (AlterTableAddColumn<?>) o;
        return Objects.equals(table, that.table) &&
               Objects.equals(addColumnDefinitions, that.addColumnDefinitions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(table, addColumnDefinitions);
    }

    @Override
    public String toString() {
        return "AlterTableAddColumn{" +
               "table=" + table +
               ", addColumnDefinition=" + addColumnDefinitions +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAlterTableAddColumnStatement(this, context);
    }
}
