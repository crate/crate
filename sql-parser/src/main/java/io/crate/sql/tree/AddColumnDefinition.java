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
import io.crate.common.collections.Lists2;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class AddColumnDefinition<T> extends TableElement<T> {

    private final T name;
    @Nullable
    private final T generatedExpression;
    @Nullable
    private final ColumnType type;
    private final List<ColumnConstraint<T>> constraints;

    public AddColumnDefinition(T name,
                               @Nullable T generatedExpression,
                               @Nullable ColumnType type,
                               List<ColumnConstraint<T>> constraints) {
        this.name = name;
        this.generatedExpression = generatedExpression;
        this.type = type;
        this.constraints = constraints;
        validateColumnDefinition();
    }

    private void validateColumnDefinition() {
        if (type == null && generatedExpression == null) {
            throw new IllegalArgumentException("Column [" + name + "]: data type needs to be provided " +
                                               "or column should be defined as a generated expression");
        }
    }

    public T name() {
        return name;
    }

    @Nullable
    public T generatedExpression() {
        return generatedExpression;
    }

    @Nullable
    public ColumnType type() {
        return type;
    }

    public List<ColumnConstraint<T>> constraints() {
        return constraints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        AddColumnDefinition that = (AddColumnDefinition) o;

        if (!name.equals(that.name)) return false;
        if (generatedExpression != null ? !generatedExpression.equals(that.generatedExpression) :
            that.generatedExpression != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return constraints.equals(that.constraints);

    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + (generatedExpression != null ? generatedExpression.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + constraints.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("generatedExpression", generatedExpression)
            .add("type", type)
            .add("constraints", constraints)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddColumnDefinition(this, context);
    }

    @Override
    public <U> AddColumnDefinition<U> map(Function<? super T, ? extends U> mapper) {
        return new AddColumnDefinition<>(
            mapper.apply(name),
            mapper.apply(generatedExpression),
            type,
            Lists2.map(constraints, x -> x.map(mapper))
        );
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
        consumer.accept(name);
        consumer.accept(generatedExpression);
        for (ColumnConstraint<T> constraint : constraints) {
            constraint.visit(consumer);
        }
    }
}
