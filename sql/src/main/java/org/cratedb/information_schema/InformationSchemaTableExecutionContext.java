package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.ITableExecutionContext;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.util.*;

public class InformationSchemaTableExecutionContext implements ITableExecutionContext {

    public static final String SCHEMA_NAME = "INFORMATION_SCHEMA";
    private final String tableName;
    private final TablesTable tablesTable = new TablesTable();

    private final Map<String, InformationSchemaTable> tableColumnMap = new HashMap<String, InformationSchemaTable>() {{
        put(TablesTable.NAME, tablesTable);
    }};

    public InformationSchemaTableExecutionContext(String tableName) {
        this.tableName = tableName;
    }

    public ImmutableMap<String, InformationSchemaColumn> fieldMapper() {
        return tableColumnMap.get(tableName).fieldMapper();
    }

    @Override
    public DocumentMapper mapper() {
        throw new UnsupportedOperationException("Information Schema currently has no DocumentMapper");
    }

    @Override
    public Object mappedValue(String name, Object value) {
        return value;
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
        return tableColumnMap.get(tableName).cols();
    }

    @Override
    public Boolean isRouting(String name) {
        return false;
    }
}