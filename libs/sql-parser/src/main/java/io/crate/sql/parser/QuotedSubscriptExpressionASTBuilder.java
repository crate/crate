/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.sql.parser;

import io.crate.sql.parser.antlr.v4.QuotedSubscriptExpressionBaseVisitor;
import io.crate.sql.parser.antlr.v4.QuotedSubscriptExpressionParser;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.IntegerLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubscriptExpression;

public class QuotedSubscriptExpressionASTBuilder extends QuotedSubscriptExpressionBaseVisitor<Node> {
    @Override
    public Node visitSubscriptExpression(QuotedSubscriptExpressionParser.SubscriptExpressionContext ctx) {
        Expression baseExpression = new QualifiedNameReference(new QualifiedName(ctx.baseColumn().getText()));
        for (var subscript : ctx.subscript()) {
            baseExpression = new SubscriptExpression(baseExpression, (Expression) visitSubscript(subscript));
        }
        return baseExpression;
    }

    @Override
    public Node visitSubscript(QuotedSubscriptExpressionParser.SubscriptContext ctx) {
        if (ctx.ARRAY_SUBSCRIPT() == null) {
            String objectSubscript = ctx.OBJECT_SUBSCRIPT().getText();
            return new StringLiteral(objectSubscript.substring(2, objectSubscript.length() - 2));
        }
        String arraySubscript = ctx.ARRAY_SUBSCRIPT().getText();
        long value = Long.parseLong(arraySubscript.substring(1, arraySubscript.length() - 1));
        if (value < Integer.MAX_VALUE + 1L) {
            return new IntegerLiteral((int) value);
        }
        return new LongLiteral(value);
    }

    @Override
    public Node visitBaseColumn(QuotedSubscriptExpressionParser.BaseColumnContext ctx) {
        return new StringLiteral(ctx.getText());
    }
}
