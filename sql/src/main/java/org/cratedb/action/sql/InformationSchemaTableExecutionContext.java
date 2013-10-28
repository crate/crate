package org.cratedb.action.sql;

import org.elasticsearch.index.mapper.DocumentMapper;

import java.util.*;

public class InformationSchemaTableExecutionContext implements ITableExecutionContext {

        private final String tableName;
        public static final String SCHEMA_NAME = "INFORMATION_SCHEMA";

        public class TablesTable {

            private List<String> cols = new ArrayList<>(3);

            public static final String NAME = "tables";

            public class Columns {
                public static final String TABLE_NAME = "table_name";
                public static final String NUMBER_OF_SHARDS = "number_of_shards";
                public static final String NUMBER_OF_REPLICAS = "number_of_replicas";
            }

            public TablesTable() {
                cols.add(Columns.TABLE_NAME);
                cols.add(Columns.NUMBER_OF_SHARDS);
                cols.add(Columns.NUMBER_OF_REPLICAS);
            }

            public Iterable<String> cols() {
                return cols;
            }
        }

        private final Map<String, Iterable<String>> tableColumnMap = new HashMap<String, Iterable<String>>() {{
            put(TablesTable.NAME, new TablesTable().cols());
        }};

        public InformationSchemaTableExecutionContext(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public List<String> primaryKeys() {
            return new ArrayList<>(0);
        }

        @Override
        public List<String> primaryKeysIncludingDefault() {
            return new ArrayList<>(0);
        }

        @Override
        public Iterable<String> allCols() {
            return tableColumnMap.get(tableName);
        }

        @Override
        public Boolean isRouting(String name) {
            return false;
        }
    }