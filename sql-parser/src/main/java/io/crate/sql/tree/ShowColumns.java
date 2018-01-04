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

import javax.annotation.Nullable;

import static com.google.common.base.Preconditions.checkNotNull;

public class ShowColumns extends Statement {

    private final QualifiedName table;
    @Nullable
    private final QualifiedName schema;
    @Nullable
    private final String likePattern;
    @Nullable
    private final Expression where;

    public ShowColumns(QualifiedName table,
                       @Nullable QualifiedName schema,
                       @Nullable Expression where,
                       @Nullable String likePattern) {
        this.table = checkNotNull(table, "table is null");
        this.schema = schema;
        this.likePattern = likePattern;
        this.where = where;
    }

    public QualifiedName table() {
        return table;
    }

    @Nullable
    public QualifiedName schema() {
        return schema;
    }

    @Nullable
    public String likePattern() {
        return likePattern;
    }

    @Nullable
    public Expression where() {
        return where;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitShowColumns(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(table, schema, likePattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ShowColumns o = (ShowColumns) obj;
        return Objects.equal(table, o.table) &&
            Objects.equal(schema, o.schema) &&
            Objects.equal(likePattern, o.likePattern) &&
            Objects.equal(where, o.where);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("table", table)
            .add("schema", schema)
            .add("pattern", likePattern)
            .add("where", where)
            .toString();
    }
}
