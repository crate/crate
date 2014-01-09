/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.crate.sql.tree;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

public class CoalesceExpression
        extends Expression
{
    private final List<Expression> operands;

    public CoalesceExpression(Expression... operands)
    {
        this(ImmutableList.copyOf(operands));
    }

    public CoalesceExpression(List<Expression> operands)
    {
        Preconditions.checkNotNull(operands, "operands is null");
        Preconditions.checkArgument(!operands.isEmpty(), "operands is empty");

        this.operands = ImmutableList.copyOf(operands);
    }

    public List<Expression> getOperands()
    {
        return operands;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitCoalesceExpression(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        CoalesceExpression that = (CoalesceExpression) o;

        if (!operands.equals(that.operands)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return operands.hashCode();
    }
}
