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


import java.util.Objects;
import java.util.Optional;

/**
 * <pre>
 * {@code
 *      <array>[<index>:<index>]
 *
 *  Examples:
 *
 *      [1,2,3][1:2]
 *      [1,2,3][1:]
 *      [1,2,3][:2]
 *      [1,2,3][:]
 * }
 * </pre>
 */
public class ArraySliceExpression extends Expression {

    private final Expression base;
    private final Optional<Expression> from;
    private final Optional<Expression> to;

    public ArraySliceExpression(Expression base, Optional<Expression> from, Optional<Expression> to) {
        this.base = base;
        this.from = from;
        this.to = to;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArraySliceExpression(this, context);
    }

    public Expression getBase() {
        return base;
    }

    public Optional<Expression> getFrom() {
        return from;
    }

    public Optional<Expression> getTo() {
        return to;
    }

    @Override
    public int hashCode() {
        return Objects.hash(base, from, to);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ArraySliceExpression other = (ArraySliceExpression) obj;
        return Objects.equals(base, other.base) && Objects.equals(from, other.from) && Objects.equals(to, other.to);
    }
}
