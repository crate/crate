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

package io.crate.analyze;

import io.crate.sql.tree.*;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;

public class ExpressionToTimeValueVisitor extends AstVisitor<TimeValue, Object[]> {

    private static final ExpressionToTimeValueVisitor INSTANCE = new ExpressionToTimeValueVisitor();
    private ExpressionToTimeValueVisitor() {}

    public static TimeValue convert(Node node, Object[] parameters) {
        return INSTANCE.process(node, parameters);
    }

    @Override
    protected TimeValue visitStringLiteral(StringLiteral node, Object[] context) {
        return TimeValue.parseTimeValue(node.getValue(), new TimeValue(0));
    }

    @Override
    protected TimeValue visitLongLiteral(LongLiteral node, Object[] context) {
        return new TimeValue(node.getValue());
    }

    @Override
    protected TimeValue visitDoubleLiteral(DoubleLiteral node, Object[] context) {
        return new TimeValue((long) node.getValue());
    }

    @Override
    protected TimeValue visitNode(Node node, Object[] context) {
        throw new IllegalArgumentException(String.format(Locale.ENGLISH, "invalid time value %s", node));
    }

}
