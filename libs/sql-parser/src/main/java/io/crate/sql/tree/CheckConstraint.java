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

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;

public class CheckConstraint<T> extends TableElement<T> {

    @Nullable
    private final String name;
    private final T expression;
    private final String expressionStr;

    public CheckConstraint(@Nullable String name, T expression, String expressionStr) {
        this.name = name;
        this.expression = expression;
        this.expressionStr = expressionStr;
    }

    @Nullable
    public String name() {
        return name;
    }


    public T expression() {
        return expression;
    }

    public <U> CheckConstraint<U> map(Function<? super T, ? extends U> mapper) {
        return new CheckConstraint<U>(name, mapper.apply(expression), expressionStr);
    }

    public String expressionStr() {
        return expressionStr;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, expression);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof CheckConstraint == false) {
            return false;
        }
        CheckConstraint<?> that = (CheckConstraint<?>) o;
        return Objects.equals(expression, that.expression) &&
               Objects.equals(name, that.name);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitCheckConstraint(this, context);
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
        consumer.accept(expression);
    }

    @Override
    public String toString() {
        return "CheckConstraint{" +
               "name='" + name + '\'' +
               ", expression=" + expression +
               '}';
    }
}
