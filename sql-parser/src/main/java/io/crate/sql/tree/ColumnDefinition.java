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
import java.util.List;

public class ColumnDefinition extends TableElement {

    private final String ident;

    @Nullable
    private final Expression defaultExpression;

    @Nullable
    private final Expression generatedExpression;

    @Nullable
    private final ColumnType type;

    private final List<ColumnConstraint> constraints;

    public ColumnDefinition(String ident,
                            @Nullable Expression defaultExpression,
                            @Nullable Expression generatedExpression,
                            @Nullable ColumnType type,
                            List<ColumnConstraint> constraints) {
        this.ident = ident;
        this.defaultExpression = defaultExpression;
        this.generatedExpression = generatedExpression;
        this.type = type;
        this.constraints = constraints;
        validateColumnDefinition();
    }

    private void validateColumnDefinition() {
        if (type == null && generatedExpression == null) {
            throw new IllegalArgumentException("Column [" + ident + "]: data type needs to be provided " +
                                               "or column should be defined as a generated expression");
        }

        if (defaultExpression != null && generatedExpression != null) {
            throw new IllegalArgumentException("Column [" + ident + "]: the default and generated expressions " +
                                               "are mutually exclusive");
        }
    }

    public String ident() {
        return ident;
    }

    @Nullable
    public Expression generatedExpression() {
        return generatedExpression;
    }

    @Nullable
    public Expression defaultExpression() {
        return defaultExpression;
    }

    @Nullable
    public ColumnType type() {
        return type;
    }

    public List<ColumnConstraint> constraints() {
        return constraints;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ident, defaultExpression, generatedExpression, type, constraints);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDefinition that = (ColumnDefinition) o;

        if (!ident.equals(that.ident)) return false;
        if (defaultExpression != null ? !defaultExpression.equals(that.defaultExpression) :
            that.defaultExpression != null) return false;
        if (generatedExpression != null ? !generatedExpression.equals(that.generatedExpression) :
            that.generatedExpression != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return constraints.equals(that.constraints);

    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("ident", ident)
            .add("defaultExpression", defaultExpression)
            .add("generatedExpression", generatedExpression)
            .add("type", type)
            .add("constraints", constraints)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitColumnDefinition(this, context);
    }
}
