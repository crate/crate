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
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;

import java.util.Locale;

public class ExpressionToMemoryValueVisitor extends AstVisitor<ByteSizeValue, Object[]> {

    private static final ExpressionToMemoryValueVisitor INSTANCE = new ExpressionToMemoryValueVisitor();

    private ExpressionToMemoryValueVisitor() {}

    public static ByteSizeValue convert(Node node, Object[] parameters) {
        return INSTANCE.process(node, parameters);
    }

    @Override
    protected ByteSizeValue visitStringLiteral(StringLiteral node, Object[] context) {
        try {
            return MemorySizeValue.parseBytesSizeValueOrHeapRatio(node.getValue());
        } catch (ElasticsearchParseException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid byte size value '%s'", node.getValue()));
        }
    }

    @Override
    protected ByteSizeValue visitLongLiteral(LongLiteral node, Object[] context) {
        return new ByteSizeValue(node.getValue());
    }

    @Override
    public ByteSizeValue visitParameterExpression(ParameterExpression node, Object[] context) {
        ByteSizeValue value;
        Object param = context[node.index()];
        if (param instanceof Number) {
            value = new ByteSizeValue(((Number) param).longValue());
        } else if (param instanceof String) {
            try {
                value = MemorySizeValue.parseBytesSizeValueOrHeapRatio((String) param);
            } catch (ElasticsearchParseException e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid byte size value '%s'", param));
            }
        } else {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid byte size value %s", param));
        }
        return value;
    }

    @Override
    protected ByteSizeValue visitNode(Node node, Object[] context) {
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid byte size value %s", node));
    }
}
