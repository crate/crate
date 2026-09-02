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

import io.crate.sql.ExpressionFormatter;

public record NegativeExpression(Expression value) implements Expression {

    /**
     * @return o * -1
     * @throws IllegalArgumentException if o is neither a Long nor a Double
     */
    public static Number negate(Object o) {
        if (o instanceof Long) {
            return -1L * (long) o;
        } else if (o instanceof Double) {
            return -1 * (double) o;
        } else {
            throw new IllegalArgumentException("Can't negate " + o);
        }
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitNegativeExpression(this, context);
    }

    @Override
    public final String toString() {
        return ExpressionFormatter.formatStandaloneExpression(this);
    }
}
