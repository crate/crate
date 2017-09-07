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

import io.crate.data.Row;
import io.crate.sql.ExpressionFormatter;
import io.crate.sql.tree.ArrayLiteral;
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.BooleanLiteral;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.NegativeExpression;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.NullLiteral;
import io.crate.sql.tree.ObjectLiteral;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.QualifiedNameReference;
import io.crate.sql.tree.StringLiteral;
import io.crate.sql.tree.SubscriptExpression;
import org.elasticsearch.common.lucene.BytesRefs;

import javax.annotation.Nullable;
import java.util.Locale;

public class ExpressionToStringVisitor extends AstVisitor<String, Row> {

    private static final ExpressionToStringVisitor INSTANCE = new ExpressionToStringVisitor();

    private ExpressionToStringVisitor() {
    }

    public static String convert(Node node, @Nullable Row context) {
        return INSTANCE.process(node, context);
    }

    @Override
    protected String visitQualifiedNameReference(QualifiedNameReference node, Row parameters) {
        return node.getName().toString();
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Row parameters) {
        return node.getValue();
    }

    @Override
    protected String visitBooleanLiteral(BooleanLiteral node, Row parameters) {
        return Boolean.toString(node.getValue());
    }

    @Override
    protected String visitDoubleLiteral(DoubleLiteral node, Row parameters) {
        return Double.toString(node.getValue());
    }

    @Override
    protected String visitLongLiteral(LongLiteral node, Row parameters) {
        return Long.toString(node.getValue());
    }

    @Override
    public String visitArrayLiteral(ArrayLiteral node, Row context) {
        return ExpressionFormatter.formatStandaloneExpression(node);
    }

    @Override
    public String visitObjectLiteral(ObjectLiteral node, Row context) {
        return ExpressionFormatter.formatStandaloneExpression(node);
    }

    @Override
    public String visitParameterExpression(ParameterExpression node, Row parameters) {
        return BytesRefs.toString(parameters.get(node.index()));
    }

    @Override
    protected String visitNegativeExpression(NegativeExpression node, Row context) {
        return "-" + process(node.getValue(), context);
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Row context) {
        return String.format(Locale.ENGLISH, "%s.%s", process(node.name(), context), process(node.index(), context));
    }

    @Override
    protected String visitNullLiteral(NullLiteral node, Row context) {
        return null;
    }

    @Override
    protected String visitNode(Node node, Row context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "Can't handle %s.", node));
    }
}
