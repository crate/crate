package org.cratedb.sql.types;

import com.google.common.base.Joiner;
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


    @SuppressWarnings("unchecked")
    public Object convertToXContentValue(String columnName,
                                         Object value) throws ColumnUnknownException,
            ValidationException {
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
                converted = columnType.toXContent(value);
            } catch (SQLType.ConvertException e) {
                throw new ValidationException(columnName, e.getMessage());
            }

            if (columnType instanceof CratySQLType) {
                for (Map.Entry<String, Object> entry : ((Map<String, Object>)converted).entrySet()) {

                    String joinedPath = Joiner.on('.').join(new String[]{columnName, entry.getKey()});
                    ((Map<String, Object>)converted).put(
                            entry.getKey(),
                            convertToXContentValue(joinedPath, entry.getValue())
                    );
                }
            }
        }
        return converted;
    }

    @SuppressWarnings("unchecked")
    public Object convertToDisplayValue(String columnName, Object value) {
        SQLType columnType = columnSqlTypes.get(columnName);
        if (columnType == null) {
            return value;
        } else {
            Object converted = columnType.toDisplayValue(value);
            if (converted != null && columnType instanceof CratySQLType) {
                // recurse
                for (Map.Entry<String, Object> entry : ((Map<String, Object>)converted).entrySet()) {
                    String joinedPath = Joiner.on('.').join(new String[]{columnName, entry.getKey()});
                    ((Map<String, Object>)converted).put(
                            entry.getKey(),
                            convertToDisplayValue(joinedPath, entry.getValue())
                    );
                }
            }
            return converted;
        }

    }
}
