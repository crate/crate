/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze.expressions;

import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.*;

import javax.annotation.Nullable;
import java.util.Locale;

public class ExpressionToStringVisitor extends AstVisitor<String, Object[]> {

    private final static ExpressionToStringVisitor INSTANCE = new ExpressionToStringVisitor();
    private ExpressionToStringVisitor() {}

    public static String convert(Node node, @Nullable Object[] context) {
        return INSTANCE.process(node, context);
    }

    @Override
    protected String visitQualifiedNameReference(QualifiedNameReference node, Object[] parameters) {
        return node.getName().toString();
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Object[] parameters) {
        return node.getValue();
    }

    @Override
    protected String visitBooleanLiteral(BooleanLiteral node, Object[] parameters) {
        return Boolean.toString(node.getValue());
    }

    @Override
    protected String visitDoubleLiteral(DoubleLiteral node, Object[] parameters) {
        return Double.toString(node.getValue());
    }

    @Override
    protected String visitLongLiteral(LongLiteral node, Object[] parameters) {
        return Long.toString(node.getValue());
    }

    @Override
    public String visitArrayLiteral(ArrayLiteral node, Object[] context) {
        return ExpressionFormatter.formatExpression(node);
    }

    @Override
    public String visitObjectLiteral(ObjectLiteral node, Object[] context) {
        return ExpressionFormatter.formatExpression(node);
    }

    @Override
    public String visitParameterExpression(ParameterExpression node, Object[] parameters) {
        return parameters[node.index()].toString();
    }

    @Override
    protected String visitNegativeExpression(NegativeExpression node, Object[] context) {
        return "-" + process(node.getValue(), context);
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Object[] context) {
        return String.format(Locale.ENGLISH, "%s.%s", process(node.name(), context), process(node.index(), context));
    }

    @Override
    protected String visitNullLiteral(NullLiteral node, Object[] context) {
        return null;
    }

    @Override
    protected String visitNode(Node node, Object[] context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle %s.", node));
    }
}
