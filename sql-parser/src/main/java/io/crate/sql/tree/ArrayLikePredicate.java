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

/**
 * ATTENTION! pattern and value swapped. use left and right
 * because they appear on wrong order in ANY LIKE predicate.
 *
 * <code><PATTERN> LIKE (<COLUMN>) [ ESCAPE <ESCAPE_VALUE> ]</code>
 *
 *
 * left is pattern
 * right is the array expression
 */
public class ArrayLikePredicate extends LikePredicate implements ArrayComparison {

    private final Quantifier quantifier;

    public ArrayLikePredicate(Quantifier quantifier,
                              Expression value,
                              Expression pattern,
                              Expression escape) {
        super(pattern, value, escape);
        this.quantifier = quantifier;
    }

    public Quantifier quantifier() {
        return quantifier;
    }

    @Override
    public Expression left() {
        return getPattern();
    }

    @Override
    public Expression right() {
        return getValue();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        ArrayLikePredicate that = (ArrayLikePredicate) o;

        if (quantifier != that.quantifier) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + quantifier.hashCode();
        return result;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitArrayLikePredicate(this, context);
    }
}
