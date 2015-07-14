package io.crate.metadata.table;

import com.google.common.collect.ImmutableMap;

public abstract class AbstractTableInfo implements TableInfo {

    private final SchemaInfo schemaInfo;

    protected AbstractTableInfo(SchemaInfo schemaInfo) {
        this.schemaInfo = schemaInfo;
    }

    @Override
    public SchemaInfo schemaInfo() {
        return schemaInfo;
    }

    @Override
    public String toString() {
        return String.format("%s.%s", schemaInfo.name(), ident().name());
    }

    @Override
    public ImmutableMap<String, Object> tableParameters() {
        return ImmutableMap.of();
    }

}
