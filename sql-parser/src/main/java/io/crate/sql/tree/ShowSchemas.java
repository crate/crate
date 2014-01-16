 
package io.crate.sql.tree;

import com.google.common.base.Objects;
import com.google.common.base.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class ShowSchemas
        extends Statement
{
    private final Optional<String> catalog;

    public ShowSchemas(Optional<String> catalog)
    {
        this.catalog = checkNotNull(catalog, "catalog is null");
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitShowSchemas(this, context);
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
