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

package io.crate.sql.tree;

import org.jspecify.annotations.Nullable;

import io.crate.sql.ExpressionFormatter;

/**
 * INTERVAL sign=(PLUS | MINUS)? stringLiteral from=intervalField (TO to=intervalField)?
 */
public record IntervalLiteral(String value,
                              Sign sign,
                              IntervalField start,
                              @Nullable IntervalField end) implements Literal {

    public enum Sign {
        PLUS,
        MINUS
    }

    public enum IntervalField {
        YEAR,
        MONTH,
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        MILLISECOND
    }

    public static String format(IntervalLiteral i) {
        StringBuilder builder = new StringBuilder("INTERVAL ");
        if (i.sign() == IntervalLiteral.Sign.MINUS) {
            builder.append("- ");
        }
        builder.append("'");
        builder.append(i.value());
        builder.append("' ")
            .append(i.start().name());
        IntervalLiteral.IntervalField endField = i.end();
        if (endField != null) {
            builder.append(" TO ").append(endField.name());
        }
        return builder.toString();
    }

    @Override
    public final String toString() {
        return ExpressionFormatter.formatStandaloneExpression(this);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitIntervalLiteral(this, context);
    }
}
