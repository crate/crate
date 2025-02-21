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

import static java.util.Objects.requireNonNull;

public class LogicalBinaryExpression extends Expression {

    public enum Type {
        AND, OR
    }

    private final Type type;
    private final Expression left;
    private final Expression right;

    public LogicalBinaryExpression(Type type, Expression left, Expression right) {
        this.type = type;
        this.left = maybeAddEqTrue(requireNonNull(left, "left is null"));
        this.right = maybeAddEqTrue(requireNonNull(right, "right is null"));
    }

    public Type getType() {
        return type;
    }

    public Expression getLeft() {
        return left;
    }

    public Expression getRight() {
        return right;
    }

    /**
     * Turns a standalone QualifiedNameReference into comparison QualifiedNameReference = true.
     * This turns statements like 'boolean_column OR expression' into 'boolean_column = true OR expression'.
     */
    private static Expression maybeAddEqTrue(Expression expression) {
        if (expression instanceof QualifiedNameReference ref) {
            return new ComparisonExpression(ComparisonExpression.Type.EQUAL, ref, BooleanLiteral.TRUE_LITERAL);
        }
        return expression;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalBinaryExpression(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalBinaryExpression that = (LogicalBinaryExpression) o;
        return type == that.type &&
               Objects.equals(left, that.left) &&
               Objects.equals(right, that.right);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, left, right);
    }
}
