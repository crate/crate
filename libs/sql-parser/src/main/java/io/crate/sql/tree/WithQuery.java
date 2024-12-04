/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

import java.util.List;
import java.util.Objects;

public class WithQuery extends Node {

    private final String name;
    private final Query query;
    private final List<String> columnNames;

    public WithQuery(String name, Query query, List<String> columnNames) {
        this.name = name;
        this.query = Objects.requireNonNull(query, "query is null");
        this.columnNames = columnNames;
    }

    public String name() {
        return name;
    }

    public Query query() {
        return query;
    }

    public List<String> columnNames() {
        return columnNames;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitWithQuery(this, context);
    }

    @Override
    public String toString() {
        return "WithQuery{" +
            "name=" + name +
            ", query=" + query +
            ", columnNames=" + columnNames +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof WithQuery that
            && name.equals(that.name)
            && query.equals(that.query)
            && Objects.equals(columnNames, that.columnNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, query, columnNames);
    }
}
