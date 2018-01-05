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

public class IndexDefinition extends TableElement {

    private final String ident;
    private final String method;
    private final List<Expression> columns;
    private final GenericProperties properties;

    public IndexDefinition(String ident, String method, List<Expression> columns, GenericProperties properties) {
        this.ident = ident;
        this.method = method;
        this.columns = columns;
        this.properties = properties;
    }

    public String ident() {
        return ident;
    }

    public String method() {
        return method;
    }

    public List<Expression> columns() {
        return columns;
    }

    public GenericProperties properties() {
        return properties;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ident, method, columns, properties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        IndexDefinition that = (IndexDefinition) o;

        if (!columns.equals(that.columns)) return false;
        if (!ident.equals(that.ident)) return false;
        if (!method.equals(that.method)) return false;
        if (!properties.equals(that.properties)) return false;

        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("ident", ident)
            .add("method", method)
            .add("columns", columns)
            .add("properties", properties)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIndexDefinition(this, context);
    }
}
