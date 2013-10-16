package org.cratedb.action.parser;

public class ColumnReferenceDescription extends ColumnDescription {

    public String name;

    public ColumnReferenceDescription(String columnName) {
        super(ColumnDescription.Types.CONSTANT_COLUMN);
        this.name = columnName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnReferenceDescription)) return false;

        ColumnReferenceDescription that = (ColumnReferenceDescription) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
