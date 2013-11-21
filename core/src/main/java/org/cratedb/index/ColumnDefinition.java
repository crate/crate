package org.cratedb.index;

public class ColumnDefinition {
    public final String tableName;
    public final String columnName;
    public final String dataType;
    public final boolean dynamic;
    public final boolean strict;
    public final int ordinalPosition;

    /**
     * Create a new ColumnDefinition
     * @param tableName the name of the table this column is in
     * @param columnName the name of the column
     * @param dataType the dataType of the column
     * @param ordinalPosition the position in the table
     * @param dynamic applies only to objects - if the type of new columns should be mapped,
     *                always false for "normal" columns
     * @param strict applied only to objects - if new columns can be added
     */
    public ColumnDefinition(String tableName, String columnName, String dataType,
                            int ordinalPosition, boolean dynamic, boolean strict) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.ordinalPosition = ordinalPosition;
        this.dynamic = dynamic;
        this.strict = strict;
    }
}
