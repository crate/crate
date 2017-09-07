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
import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.DoubleLiteral;
import io.crate.sql.tree.LongLiteral;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.ParameterExpression;
import io.crate.sql.tree.StringLiteral;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Locale;

public class ExpressionToByteSizeValueVisitor extends AstVisitor<ByteSizeValue, Row> {

    public static final ByteSizeValue DEFAULT_VALUE = new ByteSizeValue(0);
    private static final ExpressionToByteSizeValueVisitor INSTANCE = new ExpressionToByteSizeValueVisitor();

    private ExpressionToByteSizeValueVisitor() {
    }

    public static ByteSizeValue convert(Node node, Row parameters) {
        return INSTANCE.process(node, parameters);
    }

    @Override
    protected ByteSizeValue visitStringLiteral(StringLiteral node, Row context) {
        try {
            return ByteSizeValue.parseBytesSizeValue(node.getValue(), DEFAULT_VALUE.toString());
        } catch (ElasticsearchParseException e) {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Invalid byte size value '%s'", node.getValue()));
        }
    }

    @Override
    protected ByteSizeValue visitLongLiteral(LongLiteral node, Row context) {
        return new ByteSizeValue(node.getValue());
    }

    @Override
    protected ByteSizeValue visitDoubleLiteral(DoubleLiteral node, Row context) {
        return new ByteSizeValue(((Double) node.getValue()).longValue());
    }

    @Override
    public ByteSizeValue visitParameterExpression(ParameterExpression node, Row context) {
        ByteSizeValue byteSizeValue;
        Object param = context.get(node.index());
        if (param instanceof Number) {
            byteSizeValue = new ByteSizeValue(((Number) param).longValue());
        } else if (param instanceof String) {
            try {
                byteSizeValue = ByteSizeValue.parseBytesSizeValue((String) param, DEFAULT_VALUE.toString());
            } catch (ElasticsearchParseException e) {
                throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid byte size value '%s'", param));
            }
        } else {
            throw new IllegalArgumentException(
                String.format(Locale.ENGLISH, "Invalid byte size value %s", param));
        }
        return byteSizeValue;
    }

    @Override
    protected ByteSizeValue visitNode(Node node, Row context) {
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid byte size value %s", node));
    }

}
