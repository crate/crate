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

import java.util.List;
import java.util.Optional;

public class CreateTable extends Statement {

    private final Table name;
    private final List<TableElement> tableElements;
    private final boolean ifNotExists;
    private final List<CrateTableOption> crateTableOptions;
    private final Optional<GenericProperties> properties;

    public CreateTable(Table name,
                       List<TableElement> tableElements,
                       List<CrateTableOption> crateTableOptions,
                       Optional<GenericProperties> genericProperties,
                       boolean ifNotExists) {
        this.name = name;
        this.tableElements = tableElements;
        this.ifNotExists = ifNotExists;
        this.crateTableOptions = crateTableOptions;
        this.properties = genericProperties;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public Table name() {
        return name;
    }

    public List<TableElement> tableElements() {
        return tableElements;
    }

    public List<CrateTableOption> crateTableOptions() {
        return crateTableOptions;
    }

    public Optional<GenericProperties> properties() {
        return properties;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateTable(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, tableElements, crateTableOptions, properties, ifNotExists);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CreateTable that = (CreateTable) o;

        if (!name.equals(that.name)) return false;
        if (ifNotExists != that.ifNotExists) return false;
        if (!crateTableOptions.equals(that.crateTableOptions)) return false;
        if (!tableElements.equals(that.tableElements)) return false;
        if (!properties.equals(that.properties)) return false;

        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("tableElements", tableElements)
            .add("crateTableOptions", crateTableOptions)
            .add("ifNotExists", ifNotExists)
            .add("properties", properties).toString();
    }

    @Override
    public PrivilegeType privilegeType() {
        return PrivilegeType.DDL;
    }

    @Override
    public PrivilegeClazz privilegeClazz() {
        return PrivilegeClazz.SCHEMA;
    }

    @Override
    public String privilegeIdent() {
        return name.getName().getParts().get(0);
    }
}
