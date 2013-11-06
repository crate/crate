package org.cratedb.information_schema;

public interface InformationSchemaTableExecutionContextFactory {
    public InformationSchemaTableExecutionContext create(String tableName);
}
