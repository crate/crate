package org.cratedb.information_schema;

import com.google.common.collect.ImmutableMap;
import org.cratedb.action.sql.ITableExecutionContext;
import org.cratedb.service.InformationSchemaService;
import org.elasticsearch.index.mapper.DocumentMapper;

import java.util.ArrayList;
import java.util.List;

public class InformationSchemaTableExecutionContext implements ITableExecutionContext {

    public static final String SCHEMA_NAME = "INFORMATION_SCHEMA";
    private final String tableName;

    public InformationSchemaTableExecutionContext(String tableName) {
        this.tableName = tableName;
    }

    public ImmutableMap<String, InformationSchemaColumn> fieldMapper() {
        return InformationSchemaService.tables.get(tableName).fieldMapper();
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
        return InformationSchemaService.tables.get(tableName).cols();
    }

    @Override
    public Boolean isRouting(String name) {
        return false;
    }
}