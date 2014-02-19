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

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;

public class SingleColumn
        extends SelectItem
{
    private Optional<String> alias;
    private Expression expression;

    public SingleColumn(Expression expression, Optional<String> alias)
    {
        Preconditions.checkNotNull(expression, "expression is null");
        Preconditions.checkNotNull(alias, "alias is null");

        this.expression = expression;
        this.alias = alias;
    }

    public SingleColumn(Expression expression, String alias)
    {
        this(expression, Optional.of(alias));
    }

    public SingleColumn(Expression expression)
    {
        this(expression, Optional.<String>absent());
    }

    public Optional<String> getAlias()
    {
        return alias;
    }

    public void setAlias(String alias) {
        Preconditions.checkNotNull(alias);
        this.alias = Optional.of(alias);
    }

    public Expression getExpression()
    {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final SingleColumn other = (SingleColumn) obj;
        return Objects.equal(this.alias, other.alias) && Objects.equal(this.expression, other.expression);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(alias, expression);
    }

    public String toString()
    {
        if (alias.isPresent()) {
            return expression.toString() + " " + alias.get();
        }

        return expression.toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSingleColumn(this, context);
    }
}
