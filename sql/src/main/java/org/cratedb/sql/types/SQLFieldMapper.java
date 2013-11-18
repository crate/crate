package org.cratedb.sql.types;

import org.cratedb.core.IndexMetaDataExtractor;
import org.cratedb.sql.ColumnUnknownException;
import org.cratedb.sql.TypeUnknownException;
import org.cratedb.sql.ValidationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

import java.util.HashMap;
import java.util.Map;

public class SQLFieldMapper {
    private final IndexMetaDataExtractor metaDataExtractor;
    private final Map<String, SQLType> columnSqlTypes = new HashMap<>();

    @Inject
    public SQLFieldMapper(Map<String, SQLType> sqlTypes,
                          @Assisted IndexMetaDataExtractor extractor) {
        this.metaDataExtractor = extractor;

        SQLType columnType;
        for (IndexMetaDataExtractor.ColumnDefinition columnDefinition : metaDataExtractor.getColumnDefinitions()) {
            columnType = sqlTypes.get(columnDefinition.dataType);
            if (columnType == null) {
                throw new TypeUnknownException(columnDefinition.dataType);
            }
            columnSqlTypes.put(columnDefinition.columnName, columnType);
        }
    }

    public Object convertToXContentValue(String columnName,
                                         Object value) throws ColumnUnknownException, ValidationException {
        Object converted;
        SQLType columnType = columnSqlTypes.get(columnName);
        if (columnType == null) {
            if (metaDataExtractor.isDynamic()) {
                // TODO: apply dynamic mapping configurations
                return value;
            } else {
                throw new ColumnUnknownException(metaDataExtractor.getIndexName(), columnName);
            }
        } else {
            try {
                converted = columnType.toXContent(value, false);
            } catch (SQLType.ConvertException e) {
                throw new ValidationException(columnName, e.getMessage());
            }
        }
        return converted;
    }
}
