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

import com.google.common.base.Preconditions;

public class LikePredicate
    extends Expression {
    private final Expression value;
    private final Expression pattern;
    private final Expression escape;
    private final boolean ignoreCase;

    public LikePredicate(Expression value, Expression pattern, Expression escape, boolean ignoreCase) {
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkNotNull(pattern, "pattern is null");

        this.value = value;
        this.pattern = pattern;
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

        if (escape != null ? !escape.equals(that.escape) : that.escape != null) {
            return false;
        }
        if (!pattern.equals(that.pattern)) {
            return false;
        }
        if (!value.equals(that.value)) {
            return false;
        }
        if (ignoreCase != that.ignoreCase) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = value.hashCode();
        result = 31 * result + pattern.hashCode();
        result = 31 * result + (escape != null ? escape.hashCode() : 0);
        result = 31 * result + (ignoreCase ? 1 : 0);
        return result;
    }
}
