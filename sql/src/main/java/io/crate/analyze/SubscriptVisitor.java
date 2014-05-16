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
import io.crate.sql.tree.*;


public class SubscriptVisitor extends AstVisitor<Void, SubscriptContext> {

    @Override
    protected Void visitSubscriptExpression(SubscriptExpression node, SubscriptContext context) {
        Preconditions.checkArgument(
                node.index() == null || node.index() instanceof StringLiteral,
                "index of subscript has to be a string literal. Any other index expression is currently not supported"
        );
        Preconditions.checkArgument(
                node.name() instanceof SubscriptExpression || node.name() instanceof QualifiedNameReference,
                "An expression of type %s cannot have an index accessor ([])",
                node.getClass()
        );
        if (node.index() != null) {
            node.index().accept(this, context);
        }
        node.name().accept(this, context);
        return null;
    }

    @Override
    protected Void visitQualifiedNameReference(QualifiedNameReference node, SubscriptContext context) {
        context.qName(node.getName());
        return null;
    }

    @Override
    protected Void visitStringLiteral(StringLiteral node, SubscriptContext context) {
        context.add(node.getValue());
        return null;
    }

    @Override
    protected Void visitExpression(Expression node, SubscriptContext context) {
        throw new UnsupportedOperationException(String.format(
                "Expression of type %s is currently not supported within a subscript expression",
                node.getClass()));
    }
}
