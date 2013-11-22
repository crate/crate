package org.cratedb.index;

public class BuiltInColumnDefinition extends ColumnDefinition {
    /**
     * common tableName for BuiltInColumnDefinitions
     */
    public static final String VIRTUAL_SYSTEM_COLUMN_TABLE = "__";

    public static final BuiltInColumnDefinition SCORE_COLUMN =
            new BuiltInColumnDefinition("_score", "double", null, -1, false, true);

    /**
     * @param columnName      the name of the column
     * @param dataType        the dataType of the column
     * @param ordinalPosition the position in the table
     * @param dynamic         applies only to objects - if the type of new columns should be mapped,
     *                        always false for "normal" columns
     * @param strict          applied only to objects - if new columns can be added
     */
    private BuiltInColumnDefinition(String columnName, String dataType, String analyzer,
                                    int ordinalPosition, boolean dynamic, boolean strict) {
        super(VIRTUAL_SYSTEM_COLUMN_TABLE, columnName, dataType, analyzer, ordinalPosition, dynamic, strict);
    }
}
