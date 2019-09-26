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
import io.crate.common.collections.Lists2;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class ColumnDefinition<T> extends TableElement<T> {

    private final String ident;

    @Nullable
    private final T defaultExpression;

    @Nullable
    private final T generatedExpression;

    private final boolean generated;

    @Nullable
    private final ColumnType<T> type;

    private final List<ColumnConstraint<T>> constraints;

    public ColumnDefinition(String ident,
                            @Nullable T defaultExpression,
                            @Nullable T generatedExpression,
                            @Nullable ColumnType<T> type,
                            List<ColumnConstraint<T>> constraints) {
        this(ident, defaultExpression, generatedExpression, type, constraints, true, generatedExpression != null);
    }

    private ColumnDefinition(String ident,
                             @Nullable T defaultExpression,
                             @Nullable T generatedExpression,
                             @Nullable ColumnType<T> type,
                             List<ColumnConstraint<T>> constraints,
                             boolean validate,
                             boolean generated) {
        this.ident = ident;
        this.defaultExpression = defaultExpression;
        this.generatedExpression = generatedExpression;
        this.generated = generated;
        this.type = type;
        this.constraints = constraints;
        if (validate) {
            validateColumnDefinition();
        }
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

    public boolean isGenerated() {
        return generated;
    }

    @Nullable
    public T generatedExpression() {
        return generatedExpression;
    }

    @Nullable
    public T defaultExpression() {
        return defaultExpression;
    }

    @Nullable
    public ColumnType<T> type() {
        return type;
    }

    public List<ColumnConstraint<T>> constraints() {
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

    @Override
    public <U> ColumnDefinition<U> map(Function<? super T, ? extends U> mapper) {
        return new ColumnDefinition<>(
            ident,
            null, // expression must be mapped later on using mapExpressions()
            null,
            type == null ? null : type.map(mapper),
            Lists2.map(constraints, x -> x.map(mapper)),
            false,
            generatedExpression != null
        );
    }

    @Override
    public <U> TableElement<U> mapExpressions(TableElement<U> mappedElement,
                                              Function<? super T, ? extends U> mapper) {
        ColumnDefinition<U> mappedDefinition = (ColumnDefinition<U>) mappedElement;
        return new ColumnDefinition<>(
            mappedDefinition.ident,
            defaultExpression == null ? null : mapper.apply(defaultExpression),
            generatedExpression == null ? null : mapper.apply(generatedExpression),
            type == null ? null : type.mapExpressions(mappedDefinition.type, mapper),
            mappedDefinition.constraints
        );
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
        if (defaultExpression != null) {
            consumer.accept(defaultExpression);
        }
        if (generatedExpression != null) {
            consumer.accept(generatedExpression);
        }
        for (ColumnConstraint<T> constraint : constraints) {
            constraint.visit(consumer);
        }
    }
}
