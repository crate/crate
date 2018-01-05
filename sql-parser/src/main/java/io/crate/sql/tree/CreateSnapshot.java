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

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.util.Collections;
import java.util.List;

public class CreateSnapshot extends Statement {

    private final QualifiedName name;
    private final GenericProperties properties;
    private final List<Table> tableList;

    public CreateSnapshot(QualifiedName name,
                          GenericProperties genericProperties) {
        this.name = name;
        this.properties = genericProperties;
        this.tableList = Collections.emptyList();
    }

    public CreateSnapshot(QualifiedName name,
                          List<Table> tableList,
                          GenericProperties genericProperties) {
        this.name = name;
        this.tableList = tableList;
        this.properties = genericProperties;

    }

    public QualifiedName name() {
        return this.name;
    }

    public GenericProperties properties() {
        return properties;
    }

    public List<Table> tableList() {
        return tableList;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, tableList, properties);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        CreateSnapshot that = (CreateSnapshot) obj;
        if (!name.equals(that.name)) return false;
        if (!properties.equals(that.properties)) return false;
        if (!tableList.equals(that.tableList)) return false;
        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("properties", properties)
            .add("tableList", tableList)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCreateSnapshot(this, context);
    }
}
