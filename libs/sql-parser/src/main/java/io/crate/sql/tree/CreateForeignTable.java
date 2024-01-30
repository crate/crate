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
import java.util.Map;
import java.util.Objects;

public class CreateForeignTable extends Statement {

    private final QualifiedName name;
    private final boolean ifNotExists;
    private final List<TableElement<Expression>> tableElements;
    private final String server;
    private final Map<String, Expression> options;

    public CreateForeignTable(QualifiedName name,
                              boolean ifNotExists,
                              List<TableElement<Expression>> tableElements,
                              String server,
                              Map<String, Expression> options) {
        this.name = name;
        this.ifNotExists = ifNotExists;
        this.tableElements = tableElements;
        this.server = server;
        this.options = options;
    }

    public QualifiedName name() {
        return name;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public List<TableElement<Expression>> tableElements() {
        return tableElements;
    }

    public String server() {
        return server;
    }

    public Map<String, Expression> options() {
        return options;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, ifNotExists, tableElements, server);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        CreateForeignTable other = (CreateForeignTable) obj;
        return Objects.equals(name, other.name)
            && ifNotExists == other.ifNotExists
            && Objects.equals(tableElements, other.tableElements)
            && Objects.equals(server, other.server);
    }

    @Override
    public String toString() {
        return "CreateForeignTable{name=" + name + ", ifNotExists=" + ifNotExists + ", server=" + server + "}";
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateForeignTable(this, context);
    }
}
