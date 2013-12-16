package org.cratedb.index;

import org.cratedb.DataType;

public class ColumnDefinition {

    public final String tableName;
    public final String columnName;
    public final DataType dataType;
    public final String analyzer_method;
    public final boolean dynamic;
    public final boolean strict;
    public final int ordinalPosition;

    /**
     * Create a new ColumnDefinition
     * @param tableName the name of the table this column is in
     * @param columnName the name of the column
     * @param dataType the dataType of the column
     * @param analyzer_method the analyzer_method used on the column
     * @param ordinalPosition the position in the table
     * @param dynamic applies only to objects - if the type of new columns should be mapped,
     *                always false for "normal" columns
     * @param strict applied only to objects - if new columns can be added
     */
    public ColumnDefinition(String tableName, String columnName, DataType dataType, String analyzer_method,
                            int ordinalPosition, boolean dynamic, boolean strict) {
        this.tableName = tableName;
        this.columnName = columnName;
        this.dataType = dataType;
        this.analyzer_method = analyzer_method;
        this.ordinalPosition = ordinalPosition;
        this.dynamic = dynamic;
        this.strict = strict;
    }


    public boolean isMultiValued() {
        // this is not really accurate yet as fields with the plain analyzer method may still contain
        // arrays.
        // but currently it is good enough to throw an exception early in the
        // group by on fields with analyzer case

        // there might also be analyzers which don't generate multi-values and in that case
        // this check is also wrong.
        return analyzer_method != null && !analyzer_method.equals("plain");
    }
}
