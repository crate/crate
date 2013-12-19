package org.cratedb.action.parser;

import org.cratedb.DataType;
import org.cratedb.index.ColumnDefinition;

public class ColumnReferenceDescription extends ColumnDescription {

    private DataType dataType;
    private String name;


    public ColumnReferenceDescription(ColumnDefinition columnDefinition) {
        this(columnDefinition.columnName, columnDefinition.dataType);
    }

    public ColumnReferenceDescription(String columnName, DataType dataType) {
        super(ColumnDescription.Types.CONSTANT_COLUMN);
        this.name = columnName;
        this.dataType = dataType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataType returnType() {
        return dataType;
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
