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

package io.crate.analyze;

import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubqueryExpression;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.TryCast;

import java.util.Locale;

/**
 * NOTE: Array index out of bound validation cannot happen here since array index expressions such as
 * `-1` or `x + 1` are not yet evaluated. The actual validation is handled by implicit cast of
 * the array indexes to the target type (integer) of {@link io.crate.expression.scalar.SubscriptFunction}.
 * See {@link io.crate.analyze.expressions.ExpressionAnalyzer#cast}
 */
public final class SubscriptValidator {

    private SubscriptValidator() {
    }

    public static void validate(SubscriptExpression node, SubscriptContext subscriptContext) {
        node.accept(SubscriptNameVisitor.INSTANCE, subscriptContext);
    }

    private static class SubscriptNameVisitor extends AstVisitor<Void, SubscriptContext> {

        private static final SubscriptNameVisitor INSTANCE = new SubscriptNameVisitor();

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, SubscriptContext context) {
            node.index().accept(SubscriptIndexVisitor.INSTANCE, context);
            node.base().accept(this, context);
            return null;
        }

        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, SubscriptContext context) {
            context.qualifiedName(node.getName());
            return null;
        }

        @Override
        public Void visitArrayLiteral(ArrayLiteral node, SubscriptContext context) {
            context.expression(node);
            return null;
        }

        @Override
        public Void visitObjectLiteral(ObjectLiteral node, SubscriptContext context) {
            context.expression(node);
            return null;
        }

        @Override
        protected Void visitCast(Cast node, SubscriptContext context) {
            context.expression(node);
            return null;
        }

        @Override
        protected Void visitTryCast(TryCast node, SubscriptContext context) {
            context.expression(node);
            return null;
        }

        @Override
        protected Void visitFunctionCall(FunctionCall node, SubscriptContext context) {
            context.expression(node);
            return null;
        }

        @Override
        protected Void visitSubqueryExpression(SubqueryExpression node, SubscriptContext context) {
            context.expression(node);
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, SubscriptContext context) {
            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                "An expression of type %s cannot have an index accessor ([])",
                node.getClass().getSimpleName()));
        }
    }

    private static class SubscriptIndexVisitor extends AstVisitor<Void, SubscriptContext> {

        private static final SubscriptIndexVisitor INSTANCE = new SubscriptIndexVisitor();

        @Override
        public Void visitParameterExpression(ParameterExpression node, SubscriptContext context) {
            throw new UnsupportedOperationException("Parameter substitution is not supported in subscript index");
        }

        @Override
        protected Void visitStringLiteral(StringLiteral node, SubscriptContext context) {
            context.add(node.getValue());
            return null;
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, SubscriptContext context) {
            validateNestedArrayAccess(context);
            context.index(node);
            return null;
        }

        @Override
        protected Void visitIntegerLiteral(IntegerLiteral node, SubscriptContext context) {
            validateNestedArrayAccess(context);
            context.index(node);
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, SubscriptContext context) {
            context.index(node);
            return null;
        }

        private void validateNestedArrayAccess(SubscriptContext context) {
            if (context.index() != null) {
                throw new UnsupportedOperationException("Nested array access is not supported");
            }
        }
    }
}
