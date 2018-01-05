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

public class CopyFrom extends Statement {

    private final Table table;
    private final Expression path;
    private final GenericProperties genericProperties;

    public CopyFrom(Table table,
                    Expression path,
                    GenericProperties genericProperties) {

        this.table = table;
        this.path = path;
        this.genericProperties = genericProperties;
    }

    public Table table() {
        return table;
    }

    public Expression path() {
        return path;
    }

    public GenericProperties genericProperties() {
        return genericProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CopyFrom that = (CopyFrom) o;

        if (!genericProperties.equals(that.genericProperties)) return false;
        if (!path.equals(that.path)) return false;
        if (!table.equals(that.table)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + path.hashCode();
        result = 31 * result + genericProperties.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("table", table)
            .add("path", path)
            .add("properties", genericProperties)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCopyFrom(this, context);
    }
}
