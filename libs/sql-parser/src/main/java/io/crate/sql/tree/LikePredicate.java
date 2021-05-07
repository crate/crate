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

import static java.util.Objects.requireNonNull;

public class LikePredicate extends Expression {

    private final Expression value;
    private final Expression pattern;
    private final Expression escape;
    private final boolean ignoreCase;

    public LikePredicate(Expression value, Expression pattern, Expression escape, boolean ignoreCase) {

        this.value = requireNonNull(value, "value is null");
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.escape = escape;
        this.ignoreCase = ignoreCase;
    }

    public Expression getValue() {
        return value;
    }

    public Expression getPattern() {
        return pattern;
    }

    public Expression getEscape() {
        return escape;
    }

    public boolean ignoreCase() {
        return ignoreCase;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitLikePredicate(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LikePredicate that = (LikePredicate) o;
        return ignoreCase == that.ignoreCase &&
               Objects.equals(value, that.value) &&
               Objects.equals(pattern, that.pattern) &&
               Objects.equals(escape, that.escape);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, pattern, escape, ignoreCase);
    }
}
