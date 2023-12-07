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

public final class GeneratedExpressionConstraint<T> extends ColumnConstraint<T> {

    private final T expression;
    private final String expressionStr;

    public GeneratedExpressionConstraint(T expression, String expressionStr) {
        this.expression = expression;
        this.expressionStr = expressionStr;
    }

    public T expression() {
        return expression;
    }

    public String expressionStr() {
        return expressionStr;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGeneratedExpressionConstraint(this, context);
    }

    @Override
    public <U> ColumnConstraint<U> map(Function<? super T, ? extends U> mapper) {
        return new GeneratedExpressionConstraint<>(mapper.apply(expression), expressionStr);
    }

    @Override
    public void visit(Consumer<? super T> consumer) {
        consumer.accept(expression);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof GeneratedExpressionConstraint<?> that)) return false;
        return Objects.equals(expression, that.expression)
            && Objects.equals(expressionStr, that.expressionStr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(expression, expressionStr);
    }

    @Override
    public String toString() {
        return "GeneratedExpressionConstraint{" +
            "expression=" + expression +
            ", expressionStr='" + expressionStr + '\'' +
            '}';
    }
}
