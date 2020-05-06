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

import io.crate.common.collections.Lists2;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

public class PrimaryKeyConstraint<T> extends TableElement<T> {

    private final List<T> columns;

    public PrimaryKeyConstraint(List<T> columns) {
        this.columns = columns;
    }

    public List<T> columns() {
        return columns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PrimaryKeyConstraint<?> that = (PrimaryKeyConstraint<?>) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columns);
    }

    @Override
    public String toString() {
        return "PrimaryKeyConstraint{" +
               "columns=" + columns +
               '}';
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitPrimaryKeyConstraint(this, context);
    }

    @Override
    public <U> TableElement<U> map(Function<? super T, ? extends U> mapper) {
        return new PrimaryKeyConstraint<>(Lists2.map(columns, mapper));
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
        columns.forEach(consumer);
    }
}
