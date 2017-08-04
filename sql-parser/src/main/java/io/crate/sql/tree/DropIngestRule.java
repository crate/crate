package io.crate.sql.tree;


import com.google.common.base.MoreObjects;

public class DropIngestRule extends Statement {

    private final String name;
    private final boolean ifExists;

    public DropIngestRule(String name, boolean ifExists) {
        this.name = name;
        this.ifExists = ifExists;
    }

    public String name() {
        return name;
    }

    public boolean ifExists() {
        return ifExists;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        DropIngestRule that = (DropIngestRule)obj;
        if (!name.equals(that.name)) return false;
        if (ifExists != that.ifExists) return false;
        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("name", name)
            .add("ifExists", ifExists)
            .toString();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
        return visitor.visitDropIngestRule(this, context);
    }
}
