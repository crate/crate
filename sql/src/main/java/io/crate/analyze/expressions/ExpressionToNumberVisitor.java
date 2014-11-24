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

import io.crate.sql.tree.*;

import java.util.Locale;

public class ExpressionToNumberVisitor extends AstVisitor<Number, Object[]> {

    private static final ExpressionToNumberVisitor INSTANCE = new ExpressionToNumberVisitor();
    private ExpressionToNumberVisitor() {}

    public static Number convert(Node node, Object[] parameters) {
        return INSTANCE.process(node, parameters);
    }

    private Number parseString(String value) {
        Number stringNum;
        try {
            stringNum = Long.parseLong(value);
        } catch (NumberFormatException e) {
            try {
                stringNum = Double.valueOf(value);
            } catch (NumberFormatException e1) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "invalid number '%s'", value), e1);
            }
        }
        return stringNum;
    }

    @Override
    protected Number visitStringLiteral(StringLiteral node, Object[] context) {
        return parseString(node.getValue());
    }

    @Override
    protected Number visitLongLiteral(LongLiteral node, Object[] context) {
        return node.getValue();
    }

    @Override
    protected Number visitDoubleLiteral(DoubleLiteral node, Object[] context) {
        return node.getValue();
    }

    @Override
    protected Number visitNullLiteral(NullLiteral node, Object[] context) {
        return null;
    }

    @Override
    public Number visitParameterExpression(ParameterExpression node, Object[] context) {
        Number num;
        Object param = context[node.index()];
        if (param instanceof Number) {
            num = (Number)param;
        } else if (param instanceof String) {
            num = parseString((String)param);
        } else {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "invalid number %s", param));
        }
        return num;
    }

    @Override
    protected Number visitNegativeExpression(NegativeExpression node, Object[] context) {
        Number n = process(node.getValue(), context);
        if (n instanceof Long) {
            return -1L * (Long)n;
        } else if (n instanceof Double) {
            return -1 * (Double)n;
        } else {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "invalid number %s", node.getValue()));
        }
    }

    @Override
    protected Number visitNode(Node node, Object[] context) {
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "invalid number %s", node));
    }
}
