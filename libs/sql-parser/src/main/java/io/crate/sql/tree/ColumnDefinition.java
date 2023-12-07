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

public class ColumnDefinition<T> extends TableElement<T> {

    private final String ident;

    @Nullable
    private final ColumnType<T> type;

    private final List<ColumnConstraint<T>> constraints;

    public ColumnDefinition(String ident,
                            @Nullable ColumnType<T> type,
                            List<ColumnConstraint<T>> constraints) {
        this.ident = ident;
        this.type = type;
        this.constraints = constraints;
        validateColumnConstraints(ident, type, constraints);
    }

    static <T> void validateColumnConstraints(String ident,
                                              @Nullable ColumnType<T> type,
                                              List<ColumnConstraint<T>> constraints) {
        var hasConstraint = new boolean[5];
        for (ColumnConstraint<T> constraint : constraints) {
            switch (constraint) {
                case PrimaryKeyColumnConstraint<T> ignored -> {
                    if (hasConstraint[0]) {
                        throw new IllegalArgumentException("Column [" + ident + "]: multiple primary key constraints found");
                    }
                    hasConstraint[0] = true;

                }
                case IndexColumnConstraint<T> ignored -> {
                    if (hasConstraint[1]) {
                        throw new IllegalArgumentException("Column [" + ident + "]: multiple index constraints found");
                    }
                    hasConstraint[1] = true;
                }
                case DefaultConstraint<T> ignored -> {
                    if (hasConstraint[2]) {
                        throw new IllegalArgumentException("Column [" + ident + "]: multiple default constraints found");
                    }
                    hasConstraint[2] = true;
                }
                case GeneratedExpressionConstraint<T> ignored -> {
                    if (hasConstraint[3]) {
                        throw new IllegalArgumentException("Column [" + ident + "]: multiple generated constraints found");
                    }
                    hasConstraint[3] = true;
                }
                case ColumnStorageDefinition<T> ignored -> {
                    if (hasConstraint[4]) {
                        throw new IllegalArgumentException("Column [" + ident + "]: multiple storage constraints found");
                    }
                    hasConstraint[4] = true;
                }
                case CheckColumnConstraint<T> ignored -> {
                    // ignore
                }
                case NotNullColumnConstraint<T> ignored -> {
                    // ignore
                }
            }
        }

        if (type == null && hasConstraint[3] == false) {
            throw new IllegalArgumentException("Column [" + ident + "]: data type needs to be provided " +
                                               "or column should be defined as a generated expression");
        }

        if (hasConstraint[2] && hasConstraint[3]) {
            throw new IllegalArgumentException("Column [" + ident + "]: the default and generated expressions " +
                                               "are mutually exclusive");
        }
    }

    public String ident() {
        return ident;
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
        ColumnDefinition<?> that = (ColumnDefinition<?>) o;
        return Objects.equals(ident, that.ident) &&
               Objects.equals(type, that.type) &&
               Objects.equals(constraints, that.constraints);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ident, type, constraints);
    }

    @Override
    public String toString() {
        return "ColumnDefinition{" +
               "ident='" + ident + '\'' +
               ", type=" + type +
               ", constraints=" + constraints +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitColumnDefinition(this, context);
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
        for (ColumnConstraint<T> constraint : constraints) {
            constraint.visit(consumer);
        }
    }
}
