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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.ColumnType;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.FunctionCall;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubscriptExpression;
import io.crate.sql.tree.TryCast;

import java.util.Locale;

public final class SubscriptValidator {

    private static final Long MAX_VALUE = Integer.MAX_VALUE + 1L;

    private SubscriptValidator() {
    }

    public static void validate(SubscriptExpression node, SubscriptContext subscriptContext) {
        SubscriptNameVisitor.INSTANCE.process(node, subscriptContext);
    }

    private static class SubscriptNameVisitor extends AstVisitor<Void, SubscriptContext> {

        private static final SubscriptNameVisitor INSTANCE = new SubscriptNameVisitor();

        @Override
        protected Void visitSubscriptExpression(SubscriptExpression node, SubscriptContext context) {
            node.index().accept(SubscriptIndexVisitor.INSTANCE, context);
            node.name().accept(this, context);
            return null;
        }

        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, SubscriptContext context) {
            context.qualifiedName(node.getName());
            return null;
        }

        @Override
        public Void visitArrayLiteral(ArrayLiteral node, SubscriptContext context) {
            Preconditions.checkArgument(
                context.index() != null,
                "Array literals can only be accessed via numeric index.");
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
            validateNestedArrayAccess(context);
            context.add(node.getValue());
            return null;
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, SubscriptContext context) {
            validateNestedArrayAccess(context);
            long value = node.getValue();

            if (value < 1 || value > MAX_VALUE) {
                throw new UnsupportedOperationException(
                    String.format(Locale.ENGLISH, "Array index must be in range 1 to %s",
                        MAX_VALUE));
            }
            context.index(new Cast(node, new ColumnType("integer")));
            return null;
        }

        @Override
        protected Void visitNegativeExpression(NegativeExpression node, SubscriptContext context) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Array index must be in range 1 to %s",
                    MAX_VALUE));
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
