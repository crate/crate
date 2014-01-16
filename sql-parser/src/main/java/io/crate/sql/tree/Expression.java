 
package io.crate.sql.tree;

import io.crate.sql.ExpressionFormatter;

public abstract class Expression
        extends Node
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitExpression(this, context);
    }

    public final String toString()
    {
        return ExpressionFormatter.formatExpression(this);
    }
}
