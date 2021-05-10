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

package io.crate.analyze.expressions;

import io.crate.metadata.ColumnIdent;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubscriptExpression;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class ExpressionToColumnIdentVisitor extends AstVisitor<ColumnIdent, List<String>> {

    private static final ExpressionToColumnIdentVisitor INSTANCE = new ExpressionToColumnIdentVisitor();

    private ExpressionToColumnIdentVisitor() {
    }

    public static ColumnIdent convert(Node node) {
        return node.accept(INSTANCE, null);
    }

    @Override
    protected ColumnIdent visitQualifiedNameReference(QualifiedNameReference node, @Nullable List<String> context) {
        if (node.getName().getParts().size() > 1) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Qualified name references are not allowed to contain paths ('%s')",
                node.getName().toString()
            ));
        }
        if (context != null) {
            return ColumnIdent.fromNameAndPathSafe(node.getName().toString(), context);
        }
        return ColumnIdent.fromNameSafe(node.getName().toString());
    }

    @Override
    protected ColumnIdent visitStringLiteral(StringLiteral node, List<String> context) {
        if (context == null) {
            // TopLevel doesn't allow string literals.
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "String literal '%s' is not allowed as column identifier",
                node.getValue()
            ));
        }

        ColumnIdent.validateObjectKey(node.getValue());
        context.add(node.getValue());
        return null;
    }

    @Override
    protected ColumnIdent visitSubscriptExpression(SubscriptExpression node, List<String> context) {
        if (node.index() instanceof QualifiedNameReference) {
            throw new IllegalArgumentException("Key of subscript must not be a reference");
        }
        if (context == null) {
            context = new ArrayList<>();
        }
        ColumnIdent colIdent = node.base().accept(this, context);
        node.index().accept(this, context);


        return colIdent;
    }

    @Override
    protected ColumnIdent visitNode(Node node, List<String> context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle %s.", node));
    }
}
