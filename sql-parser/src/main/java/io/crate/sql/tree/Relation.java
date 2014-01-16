 
package io.crate.sql.tree;

public abstract class Relation
        extends Node
{
    @Override
    public abstract int hashCode();

    @Override
    public abstract boolean equals(Object obj);

    @Override
    public abstract String toString();

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRelation(this, context);
    }
}
