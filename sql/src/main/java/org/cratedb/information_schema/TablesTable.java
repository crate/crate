package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;

import java.util.LinkedHashMap;
import java.util.Map;

public class TablesTable implements InformationSchemaTable {

    private Map<String, InformationSchemaColumn> fieldMapper = new LinkedHashMap<>();

    public static final String NAME = "tables";

    public class Columns {
        public static final String TABLE_NAME = "table_name";
        public static final String NUMBER_OF_SHARDS = "number_of_shards";
        public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    }

    public TablesTable() {
        fieldMapper.put(
            Columns.TABLE_NAME,
            new InformationSchemaStringColumn(Columns.TABLE_NAME)
        );

        fieldMapper.put(
            Columns.NUMBER_OF_SHARDS,
            new InformationSchemaIntegerColumn(Columns.NUMBER_OF_SHARDS)
        );

        fieldMapper.put(
            Columns.NUMBER_OF_REPLICAS,
            new InformationSchemaIntegerColumn(Columns.NUMBER_OF_REPLICAS)
        );
    }

    public Iterable<String> cols() {
        return fieldMapper.keySet();
    }

    public ImmutableMap<String, InformationSchemaColumn> fieldMapper() {
        return ImmutableMap.copyOf(fieldMapper);
    }
}
