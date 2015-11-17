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
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import java.util.List;

public class GeneratedColumnDefinition extends TableElement {

    private final String ident;
    private final Expression expression;
    @Nullable
    private final ColumnType type;
    private final List<ColumnConstraint> constraints;

    public GeneratedColumnDefinition(String ident,
                                     Expression expression,
                                     @Nullable ColumnType type,
                                     @Nullable List<ColumnConstraint> constraints) {
        this.ident = ident;
        this.expression = expression;
        this.type = type;
        this.constraints = MoreObjects.firstNonNull(constraints, ImmutableList.<ColumnConstraint>of());
    }

    public GeneratedColumnDefinition(String ident,
                                     Expression expression,
                                     @Nullable List<ColumnConstraint> constraints) {
        this(ident, expression, null, constraints);
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

    public List<ColumnConstraint> constraints() {
        return constraints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GeneratedColumnDefinition that = (GeneratedColumnDefinition) o;

        if (!ident.equals(that.ident)) return false;
        if (!expression.equals(that.expression)) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return constraints.equals(that.constraints);

    }

    @Override
    public int hashCode() {
        int result = ident.hashCode();
        result = 31 * result + expression.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + constraints.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "GeneratedColumnDefinition{" +
               "ident='" + ident + '\'' +
               ", expression=" + expression +
               ", type=" + type +
               ", constraints=" + constraints +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGeneratedColumnDefinition(this, context);
    }
}
