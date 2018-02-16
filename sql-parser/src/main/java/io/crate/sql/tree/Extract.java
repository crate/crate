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

package io.crate.sql.tree;

import javax.annotation.concurrent.Immutable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class Extract
    extends Expression {
    private final Expression expression;
    private final Expression field;

    public enum Field {
        CENTURY,
        YEAR,
        QUARTER,
        MONTH,
        WEEK,
        DAY,
        DAY_OF_MONTH,
        DAY_OF_WEEK,
        DOW,
        DAY_OF_YEAR,
        DOY,
        HOUR,
        MINUTE,
        SECOND,
        TIMEZONE_HOUR,
        TIMEZONE_MINUTE,
        EPOCH
    }

    public Extract(Expression expression, Expression field) {
        checkNotNull(expression, "expression is null");
        checkNotNull(field, "field is null");
        checkArgument(field instanceof StringLiteral || field instanceof ParameterExpression,
            // (ident is converted to StringLiteral in StatementBuilder.g
            "field must be an ident, a string literal or a parameter expression");
        this.expression = expression;
        this.field = field;
    }

    public Expression getExpression() {
        return expression;
    }

    public Expression getField() {
        return field;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitExtract(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Extract that = (Extract) o;

        if (!expression.equals(that.expression)) {
            return false;
        }
        if (!field.equals(that.field)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = expression.hashCode();
        result = 31 * result + field.hashCode();
        return result;
    }
}
