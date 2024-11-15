/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.sql.tree;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class GroupBy extends Node {

    private final boolean isAll;
    private final List<Expression> expressions;

    private GroupBy(boolean isAll, List<Expression> expressions) {
        this.isAll = isAll;
        this.expressions = requireNonNull(expressions, "expressions is null");
    }

    public static GroupBy all() {
        return new GroupBy(true, List.of());
    }

    public static GroupBy of(List<Expression> expressions) {
        return new GroupBy(false, expressions);
    }

    public boolean isAll() {
        return isAll;
    }

    public List<Expression> getExpressions() {
        return expressions;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitGroupBy(this, context);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GroupBy groupBy = (GroupBy) o;
        return isAll == groupBy.isAll &&
            Objects.equals(expressions, groupBy.expressions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Boolean.valueOf(isAll), expressions);
    }

    @Override
    public String toString() {
        return "GroupBy{" +
            "isAll=" + isAll +
            ", expressions=" + expressions +
            '}';
    }
}
