package org.cratedb.action.parser;

public class ColumnReferenceDescription extends ColumnDescription {

    public String name;

    public ColumnReferenceDescription(String columnName) {
        super(ColumnDescription.Types.CONSTANT_COLUMN);
        this.name = columnName;
    }
}
