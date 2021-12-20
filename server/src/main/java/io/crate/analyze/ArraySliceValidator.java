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
import io.crate.sql.tree.ArraySliceExpression;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Cast;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.TryCast;

import java.util.Locale;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

public final class ArraySliceValidator {

    private static final long MAX_VALUE = Integer.MAX_VALUE;

    private ArraySliceValidator() {
    }

    public static void validate(ArraySliceExpression node, ArraySliceContext arraySliceContext) {
        node.accept(ArraySliceNameVisitor.INSTANCE, arraySliceContext);
    }

    private static class ArraySliceNameVisitor extends AstVisitor<Void, ArraySliceContext> {

        private static final ArraySliceNameVisitor INSTANCE = new ArraySliceNameVisitor();

        @Override
        protected Void visitArraySliceExpression(ArraySliceExpression node, ArraySliceContext context) {
            node.getFrom().ifPresent(from -> from.accept(ArraySliceIndexVisitor.FROM_INSTANCE, context));
            node.getTo().ifPresent(to -> to.accept(ArraySliceIndexVisitor.TO_INSTANCE, context));
            node.getBase().accept(this, context);
            return null;
        }

        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, ArraySliceContext context) {
            context.setQualifiedName(node.getName());
            return null;
        }

        @Override
        protected Void visitCast(Cast node, ArraySliceContext context) {
            context.setBase(node);
            return null;
        }

        @Override
        protected Void visitTryCast(TryCast node, ArraySliceContext context) {
            context.setBase(node);
            return null;
        }

        @Override
        public Void visitArrayLiteral(ArrayLiteral node, ArraySliceContext context) {
            context.setBase(node);
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, ArraySliceContext context) {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH,
                              "An expression of type %s cannot have an array slice ([<from>:<to>])",
                              node.getClass().getSimpleName())
            );
        }
    }

    private static class ArraySliceIndexVisitor extends AstVisitor<Void, ArraySliceContext> {

        private static final ArraySliceIndexVisitor FROM_INSTANCE = new ArraySliceIndexVisitor(
            ArraySliceContext::setFrom,
            ArraySliceContext::getFrom
        );
        private static final ArraySliceIndexVisitor TO_INSTANCE = new ArraySliceIndexVisitor(
            ArraySliceContext::setTo,
            ArraySliceContext::getTo
        );

        private final BiConsumer<ArraySliceContext, Optional<Expression>> setter;
        private final Function<ArraySliceContext, Optional<Expression>> getter;

        public ArraySliceIndexVisitor(BiConsumer<ArraySliceContext, Optional<Expression>> setter,
                                      Function<ArraySliceContext, Optional<Expression>> getter) {
            this.setter = setter;
            this.getter = getter;
        }

        private void validateNestedArraySlice(ArraySliceContext context) {
            if (getter.apply(context).isPresent()) {
                throw new UnsupportedOperationException("Nested array slice is not supported");
            }
        }

        @Override
        public Void visitParameterExpression(ParameterExpression node, ArraySliceContext context) {
            throw new UnsupportedOperationException("Parameter substitution is not supported in an array slice");
        }

        @Override
        protected Void visitLongLiteral(LongLiteral node, ArraySliceContext context) {
            validateNestedArraySlice(context);
            long value = node.getValue();

            if (value < 1 || value > MAX_VALUE) {
                raiseInvalidIndexValue();
            }
            setter.accept(context, Optional.of(new IntegerLiteral((int) value)));
            return null;
        }

        @Override
        protected Void visitIntegerLiteral(IntegerLiteral node, ArraySliceContext context) {
            validateNestedArraySlice(context);
            setter.accept(context, Optional.of(node));
            return null;
        }

        @Override
        protected Void visitExpression(Expression node, ArraySliceContext context) {
            validateNestedArraySlice(context);
            setter.accept(context, Optional.of(node));
            return null;
        }

        @Override
        protected Void visitNegativeExpression(NegativeExpression node, ArraySliceContext context) {
            raiseInvalidIndexValue();
            return null;
        }
    }

    public static void raiseInvalidIndexValue() {
        throw new UnsupportedOperationException(
            String.format(Locale.ENGLISH, "Array index must be in range 1 to %s", Integer.MAX_VALUE)
        );
    }
}
