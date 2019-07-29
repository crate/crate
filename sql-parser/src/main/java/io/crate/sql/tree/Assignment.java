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
import com.google.common.base.Preconditions;
import io.crate.common.collections.Lists2;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class Assignment<T> extends Node {

    private final T columnName;
    private final List<T> expressions;

    /**
     * Constructor for SET SESSION/LOCAL statements
     * one or more expressions are allowed on the right side of the assignment
     * DEFAULT             -> empty list of expressions
     * VALUE               -> single item in expressions list
     *                        value can be either string literal, numeric literal, or ident
     * VALUE, VALUE, ...   -> two or more items in expressions list
     */
    public Assignment(T columnName, List<T> expressions) {
        Preconditions.checkNotNull(columnName, "columnname is null");
        Preconditions.checkNotNull(expressions, "expression is null");
        this.columnName = columnName;
        this.expressions = expressions;
    }

    /**
     * Constructor for SET GLOBAL statements
     * only single expression is allowed on right side of assignment
     */
    public Assignment(T columnName, T expression) {
        Preconditions.checkNotNull(columnName, "columnname is null");
        Preconditions.checkNotNull(expression, "expression is null");
        this.columnName = columnName;
        this.expressions = Collections.singletonList(expression);
    }

    public T columnName() {
        return columnName;
    }

    public T expression() {
        return expressions.isEmpty() ? null : expressions.get(0);
    }

    public List<T> expressions() {
        return expressions;
    }

    public <U> Assignment<U> map(Function<? super T, ? extends U> mapper) {
        return new Assignment<>(mapper.apply(columnName), Lists2.map(expressions, mapper));
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(columnName, expressions);
    }


    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("column", columnName)
            .add("expressions", expressions)
            .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Assignment that = (Assignment) o;

        if (!columnName.equals(that.columnName)) return false;
        if (!expressions.equals(that.expressions)) return false;

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitAssignment(this, context);
    }
}
