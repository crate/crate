package org.cratedb.sql.types;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import org.cratedb.core.IndexMetaDataExtractor;
import org.cratedb.sql.ColumnUnknownException;
import org.cratedb.sql.TypeUnknownException;
import org.cratedb.sql.ValidationException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SQLFieldMapper {
    private final IndexMetaDataExtractor metaDataExtractor;
    private final Map<String, Tuple<IndexMetaDataExtractor.ColumnDefinition, SQLType>> columnSqlTypes = new HashMap<>();

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
            columnSqlTypes.put(columnDefinition.columnName, new Tuple<>(columnDefinition, columnType));
        }
    }


    @SuppressWarnings("unchecked")
    public Object convertToXContentValue(String columnName,
                                         Object value) throws ColumnUnknownException,
            ValidationException {
        Object converted;
        Tuple<IndexMetaDataExtractor.ColumnDefinition, SQLType> columnAndType = columnSqlTypes.get(columnName);
        if (columnAndType == null) {
            columnAndType = getParentColumnAndType(columnName);
            if ((columnAndType != null && columnAndType.v1().dynamic)
                        ||
                (columnAndType == null && metaDataExtractor.isDynamic())) {
                // TODO: apply dynamic mapping configurations - type guessing
                return value;
            } else {
                throw new ColumnUnknownException(metaDataExtractor.getIndexName(), columnName);
            }

        } else {
            try {
                converted = columnAndType.v2().toXContent(value);
            } catch (SQLType.ConvertException e) {
                throw new ValidationException(columnName, e.getMessage());
            }

            if (columnAndType.v2() instanceof CratySQLType) {
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

    /**
     * returns the parent ColumnDefinition and its SQLType of the given Column, if one exists
     *
     * @param columnName the name of the column to get its parent from
     * @return a Tuple of ColumnDefinition and SQLType or null
     */
    private Tuple<IndexMetaDataExtractor.ColumnDefinition, SQLType> getParentColumnAndType(String columnName) {
        List<String> pathElements = Lists.newArrayList(Splitter.on('.').split(columnName));
        if (pathElements.size()>1) {
            String joined = Joiner.on('.').join(pathElements.subList(0, pathElements.size()-1));
            return columnSqlTypes.get(joined);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Object convertToDisplayValue(String columnName, Object value) {
        Tuple<IndexMetaDataExtractor.ColumnDefinition, SQLType> columnAndType = columnSqlTypes.get(columnName);
        if (columnAndType.v2() == null) {
            return value;
        } else {
            Object converted = columnAndType.v2().toDisplayValue(value);
            if (converted != null && columnAndType.v2() instanceof CratySQLType) {
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
