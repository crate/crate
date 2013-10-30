package org.cratedb.information_schema;

import org.cratedb.action.sql.ITableExecutionContext;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.util.*;

public class InformationSchemaTableExecutionContext implements ITableExecutionContext {

    private final String tableName;
    public static final String SCHEMA_NAME = "INFORMATION_SCHEMA";

    private final Map<String, Iterable<String>> tableColumnMap = new HashMap<String, Iterable<String>>() {{
        put(TablesTable.NAME, new TablesTable().cols());
        put(TableConstraintsTable.NAME, new TableConstraintsTable().cols());
    }};

    public InformationSchemaTableExecutionContext(String tableName) {
        this.tableName = tableName;
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
        return tableColumnMap.get(tableName);
    }

    @Override
    public Boolean isRouting(String name) {
        return false;
    }
}