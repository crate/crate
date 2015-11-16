/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;

import javax.annotation.Nullable;

public class GeneratedColumnDefinition extends TableElement {

    private final String ident;
    private final Expression expression;
    @Nullable
    private final ColumnType type;

    public GeneratedColumnDefinition(String ident, Expression expression, @Nullable ColumnType type) {
        this.ident = ident;
        this.expression = expression;
        this.type = type;
    }

    public GeneratedColumnDefinition(String ident, Expression expression) {
        this(ident, expression, null);
    }

    public String ident() {
        return ident;
    }

    public Expression expression() {
        return expression;
    }

    @Nullable
    public ColumnType type() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeneratedColumnDefinition that = (GeneratedColumnDefinition) o;

        if (!ident.equals(that.ident)) return false;
        if (!expression.equals(that.expression)) return false;
        return !(type != null ? !type.equals(that.type) : that.type != null);

    }

    @Override
    public int hashCode() {
        int result = ident.hashCode();
        result = 31 * result + expression.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("ident", ident)
                .add("type", type)
                .add("expression", expression)
                .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGeneratedColumnDefinition(this, context);
    }
}
