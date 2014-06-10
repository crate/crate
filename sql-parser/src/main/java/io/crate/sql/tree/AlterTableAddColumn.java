/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

public class AlterTableAddColumn extends Statement {

    private final TableElement tableElement;
    private final Table table;

    public AlterTableAddColumn(Table table, TableElement tableElement) {
        this.table = table;
        this.tableElement = tableElement;
    }

    public TableElement tableElement() {
        return tableElement;
    }

    public Table table() {
        return table;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof AlterTableAddColumn)) return false;

        AlterTableAddColumn that = (AlterTableAddColumn) o;

        if (!table.equals(that.table)) return false;
        if (!tableElement.equals(that.tableElement)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = tableElement.hashCode();
        result = 31 * result + table.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("element", tableElement).toString();
    }
}
