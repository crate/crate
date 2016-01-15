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
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;

public class ExpressionToTimeValueVisitor {

    public static final TimeValue DEFAULT_VALUE = new TimeValue(0);
    private static final Visitor VISITOR = new Visitor();

    private ExpressionToTimeValueVisitor() {}

    public static TimeValue convert(Node node, Object[] parameters, String settingName) {
        return VISITOR.process(node, new Context(settingName, parameters));
    }

    private static class Context {
        final String settingName;
        final Object[] params;

        public Context(String settingName, Object[] params) {
            this.settingName = settingName;
            this.params = params;
        }
    }

    private static class Visitor extends AstVisitor<TimeValue, Context> {

        @Override
        protected TimeValue visitStringLiteral(StringLiteral node, Context context) {
            try {
                //TODO: FIX ME! parseTimeValue requires setting
                return TimeValue.parseTimeValue(node.getValue(), DEFAULT_VALUE, "");
            } catch (ElasticsearchParseException e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid time value '%s'", node.getValue()));
            }
        }

        @Override
        protected TimeValue visitLongLiteral(LongLiteral node, Context context) {
            return new TimeValue(node.getValue());
        }

        @Override
        protected TimeValue visitDoubleLiteral(DoubleLiteral node, Context context) {
            return new TimeValue((long) node.getValue());
        }

        @Override
        public TimeValue visitParameterExpression(ParameterExpression node, Context context) {
            TimeValue timeValue;
            Object param = context.params[node.index()];
            if (param instanceof Number) {
                timeValue = new TimeValue(((Number) param).longValue());
            } else if (param instanceof String) {
                try {
                    //TODO: FIX ME! parseTimeValue requires setting
                    timeValue = TimeValue.parseTimeValue((String) param, DEFAULT_VALUE, "");
                } catch (ElasticsearchParseException e) {
                    throw new IllegalArgumentException(
                            String.format(Locale.ENGLISH, "Invalid time value '%s'", param));
                }
            } else {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid time value %s", param));
            }
            return timeValue;
        }

        @Override
        protected TimeValue visitNode(Node node, Context context) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "Invalid time value %s", node));
        }
    }
}
