 
package io.crate.sql.tree;

import com.google.common.base.Objects;

public final class ShowCatalogs
        extends Statement
{
    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowCatalogs(this, context);
    }

    @Override
    public int hashCode()
    {
        return 0;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        return (obj != null) && (getClass() == obj.getClass());
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this).toString();
    }
}
