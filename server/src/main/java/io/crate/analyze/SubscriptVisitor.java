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

import java.util.Locale;

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

/**
 * Utility class to extract index and subscript information from a {@link SubscriptExpression}
 */
public final class SubscriptVisitor {

    private SubscriptVisitor() {
    }

    /**
     * Extracts index and subscript information from the passed in SubscriptExpression
     * </p>
     * Note that array index out of bound validation does not happen here, as index
     * expressions such as `-1` or `x + 1` are not yet evaluated.  This will happen instead
     * during the implicit cast of indexes to integers by a {@link io.crate.expression.scalar.SubscriptFunction}
     *
     * @param node  the node to analyse
     * @return      a {@link SubscriptContext} containing the index and subscript information
     */
    public static SubscriptContext visit(Expression node) {
        SubscriptContext subscriptContext = new SubscriptContext();
        node.accept(SubscriptNameVisitor.INSTANCE, subscriptContext);
        return subscriptContext;
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
            context.addKey(node.getValue());
            return null;
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, SubscriptContext context) {
            context.addIndex(node);
            return null;
        }

        @Override
        protected Void visitIntegerLiteral(IntegerLiteral node, SubscriptContext context) {
            context.addIndex(node);
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, SubscriptContext context) {
            context.addIndex(node);
            return null;
        }
    }
}
