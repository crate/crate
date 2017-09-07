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

package io.crate.analyze.expressions;


import io.crate.data.Row;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NullLiteral;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubscriptExpression;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class ExpressionToObjectVisitor extends AstVisitor<Object, Row> {

    private static final ExpressionToObjectVisitor INSTANCE = new ExpressionToObjectVisitor();

    private ExpressionToObjectVisitor() {
    }

    public static Object convert(Node node, Row parameters) {
        return INSTANCE.process(node, parameters);
    }

    @Override
    protected String visitQualifiedNameReference(QualifiedNameReference node, Row parameters) {
        return node.getName().getSuffix();
    }

    @Override
    protected Object visitBooleanLiteral(BooleanLiteral node, Row context) {
        return node.getValue();
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Row parameters) {
        return node.getValue();
    }

    @Override
    public Object visitParameterExpression(ParameterExpression node, Row parameters) {
        return parameters.get(node.index());
    }

    @Override
    protected Object visitLongLiteral(LongLiteral node, Row context) {
        return node.getValue();
    }

    @Override
    protected Object visitDoubleLiteral(DoubleLiteral node, Row context) {
        return node.getValue();
    }

    @Override
    protected Object visitNullLiteral(NullLiteral node, Row context) {
        return null;
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Row context) {
        return String.format(Locale.ENGLISH, "%s.%s", process(node.name(), context), process(node.index(), context));
    }

    @Override
    public Object[] visitArrayLiteral(ArrayLiteral node, Row context) {
        Object[] array = new Object[node.values().size()];
        for (int i = 0; i < node.values().size(); i++) {
            array[i] = node.values().get(i).accept(this, context);
        }
        return array;
    }

    @Override
    public Map<String, Object> visitObjectLiteral(ObjectLiteral node, Row context) {
        Map<String, Object> object = new HashMap<>();
        for (Map.Entry<String, Expression> entry : node.values().entries()) {
            if (object.put(entry.getKey(), entry.getValue().accept(this, context)) != null) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH,
                        "key '%s' listed twice in object literal",
                        entry.getKey())
                );
            }
        }
        return object;
    }

    @Override
    protected Object visitNegativeExpression(NegativeExpression node, Row context) {
        Object o = process(node.getValue(), context);
        if (o instanceof Long) {
            return -1L * (Long) o;
        } else if (o instanceof Double) {
            return -1 * (Double) o;
        } else {
            throw new UnsupportedOperationException(
                String.format(Locale.ENGLISH, "Can't handle negative of %s.", node.getValue()));
        }
    }

    @Override
    protected Object visitNode(Node node, Row context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle %s.", node));
    }
}
