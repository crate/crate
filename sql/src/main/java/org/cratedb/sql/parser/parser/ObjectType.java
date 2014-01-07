package org.cratedb.sql.parser.parser;

/**
 * Object Type for Object Columns
 */
public enum ObjectType {
    DYNAMIC,
    STRICT,
    IGNORED;

    public static ObjectType getByName(String name) {
        return ObjectType.valueOf(name.toUpperCase());
    }
}
