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

import java.util.List;

public class Insert extends Statement {

    private final Table table;
    private final List<List<Expression>> valuesList;
    private final List<QualifiedName> columns;

    public Insert(Table table, List<List<Expression>> valuesList, List<QualifiedName> columns) {
        this.table = table;
        this.valuesList = valuesList;
        this.columns = columns;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, valuesList, columns);
    }


    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", table)
                .add("valuesList", valuesList)
                .add("columns", Objects.firstNonNull(columns, "null"))
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Insert insert = (Insert) o;

        if (columns != null ? !columns.equals(insert.columns) : insert.columns != null)
            return false;
        if (table != null ? !table.equals(insert.table) : insert.table != null)
            return false;
        if (valuesList != null ? !valuesList.equals(insert.valuesList) : insert.valuesList != null) return false;

        return true;
    }
}
