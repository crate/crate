package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * virtual information_schema table listing table constraints like primary_key constraints
 */
public class TableConstraintsTable implements InformationSchemaTable {

    private Map<String, InformationSchemaColumn> fieldMapper = new LinkedHashMap<>();

    public static final String NAME = "table_constraints";

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String CONSTRAINT_NAME = "constraint_name";
        public static final String CONSTRAINT_TYPE = "constraint_type";
    }

    public class ConstraintType {
        public static final String PRIMARY_KEY = "PRIMARY_KEY";
        public static final String UNIQUE = "UNIQUE";
        public static final String CHECK = "CHECK";
    }

    public TableConstraintsTable() {
        fieldMapper.put(
                Columns.TABLE_NAME,
                new InformationSchemaStringColumn(Columns.TABLE_NAME)
        );
        fieldMapper.put(
                Columns.CONSTRAINT_NAME,
                new InformationSchemaStringColumn(Columns.CONSTRAINT_NAME)
        );
        fieldMapper.put(
                Columns.CONSTRAINT_TYPE,
                new InformationSchemaStringColumn(Columns.CONSTRAINT_TYPE)
        );
    }

    @Override
    public Iterable<String> cols() {
        return fieldMapper.keySet();
    }

    @Override
    public ImmutableMap<String, InformationSchemaColumn> fieldMapper() {
        return ImmutableMap.copyOf(fieldMapper);
    }
}
