package org.cratedb.index;

import java.util.ArrayList;
import java.util.List;

/*
 * Columndefinition that contains other columns
 */
public class ObjectColumnDefinition extends ColumnDefinition {
    public final List<ColumnDefinition> nestedColumns = new ArrayList<>();


    public ObjectColumnDefinition(String tableName, String columnName, String dataType, int ordinalPosition, boolean dynamic, boolean strict) {
        super(tableName, columnName, dataType, ordinalPosition, dynamic, strict);
    }
}
