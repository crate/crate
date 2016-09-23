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

package io.crate.sql.tree;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

public class MatchPredicateColumnIdent extends Expression {

    private final Expression ident;
    private final Expression boost;

    public MatchPredicateColumnIdent(Expression ident, @Nullable Expression boost) {
        this.ident = ident;
        if (boost != null) {
            Preconditions.checkArgument(
                boost instanceof LongLiteral || boost instanceof DoubleLiteral || boost instanceof ParameterExpression,
                "'boost' value must be a numeric literal or a parameter expression");
        }
        this.boost = MoreObjects.firstNonNull(boost, NullLiteral.INSTANCE);
    }

    public Expression columnIdent() {
        return ident;
    }

    public Expression boost() {
        return boost;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(ident, boost);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MatchPredicateColumnIdent that = (MatchPredicateColumnIdent) o;

        if (!ident.equals(that.ident)) {
            return false;
        }
        if (!boost.equals(that.boost)) {
            return false;
        }

        return true;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitMatchPredicateColumnIdent(this, context);
    }
}
