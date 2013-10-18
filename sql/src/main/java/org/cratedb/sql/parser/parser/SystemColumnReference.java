package org.cratedb.sql.parser.parser;


import org.cratedb.sql.SQLParseException;

/**
 * Marker Class for SystemColumnReferences
 *
 * always starting with an underscore
 */
public class SystemColumnReference extends ColumnReference {

    public static enum SystemColumnName {
        VERSION("_version"),
        TTL("_ttl");

        private final String name;

        private SystemColumnName(String name) {
            this.name = name;
        }

        public static boolean contains(String value) {
            for (SystemColumnName name : values()) {
                if (name.name.equals(value)) { return true; }
            }
            return false;
        }
    }

    @Override
    public void init(Object columnName, Object tableName) {
        if (!SystemColumnName.contains((String)columnName)) {
            throw new SQLParseException("Invalid SystemColumnReference");
        }
        super.init(columnName, tableName);
    }

    @Override
    public void init(Object columnName, Object tableName, Object tokBeginOffset, Object tokEndOffset) {
        if (!SystemColumnName.contains((String)columnName)) {
            throw new SQLParseException("Invalid SystemColumnReference");
        }
        super.init(columnName, tableName, tokBeginOffset, tokEndOffset);
    }
}
