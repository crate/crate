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
import java.util.Objects;
import java.util.function.Consumer;

import org.jetbrains.annotations.Nullable;

public class AddColumnDefinition<T> extends TableElement<T> {

    private final T name;
    @Nullable
    private final T generatedExpression;
    @Nullable
    private final ColumnType<T> type;
    private final List<ColumnConstraint<T>> constraints;

    public AddColumnDefinition(T name,
                               @Nullable T generatedExpression,
                               @Nullable ColumnType<T> type,
                               List<ColumnConstraint<T>> constraints) {
        this.name = name;
        this.generatedExpression = generatedExpression;
        this.type = type;
        this.constraints = constraints;
        if (type == null && generatedExpression == null) {
            throw new IllegalArgumentException(
                "Column [" + name + "]: data type needs to be provided " +
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
    public ColumnType<T> type() {
        return type;
    }

    public List<ColumnConstraint<T>> constraints() {
        return constraints;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AddColumnDefinition<?> that = (AddColumnDefinition<?>) o;
        return Objects.equals(name, that.name) &&
               Objects.equals(generatedExpression, that.generatedExpression) &&
               Objects.equals(type, that.type) &&
               Objects.equals(constraints, that.constraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, generatedExpression, type, constraints);
    }

    @Override
    public String toString() {
        return "AddColumnDefinition{" +
               "name=" + name +
               ", generatedExpression=" + generatedExpression +
               ", type=" + type +
               ", constraints=" + constraints +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAddColumnDefinition(this, context);
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
