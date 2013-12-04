package org.cratedb.sql.types;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.cratedb.DataType;
import org.cratedb.index.BuiltInColumnDefinition;
import org.cratedb.index.ColumnDefinition;
import org.cratedb.index.IndexMetaDataExtractor;
import org.cratedb.mapper.FieldMapper;
import org.cratedb.sql.ColumnUnknownException;
import org.cratedb.sql.TypeUnknownException;
import org.cratedb.sql.ValidationException;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class SQLFieldMapper implements FieldMapper {
    private final IndexMetaDataExtractor metaDataExtractor;
    private final Map<String, Tuple<ColumnDefinition, SQLType>> columnSqlTypes = new HashMap<>();
    private static final Pattern numberPattern = Pattern.compile("\\d+");
    private static final ImmutableMap<String, Tuple<ColumnDefinition, SQLType>> builtInColumnTypes =
            new ImmutableMap.Builder<String, Tuple<ColumnDefinition, SQLType>>()
            .put(
                    "_score",
                    new Tuple<>((ColumnDefinition)BuiltInColumnDefinition.SCORE_COLUMN,
                        (SQLType)new DoubleSQLType()))
            .build();

    @Inject
    public SQLFieldMapper(Map<DataType, SQLType> sqlTypes,
                          @Assisted IndexMetaDataExtractor extractor) {
        this.metaDataExtractor = extractor;

        columnSqlTypes.putAll(builtInColumnTypes);
        SQLType columnType;
        for (ColumnDefinition columnDefinition : metaDataExtractor.getColumnDefinitions()) {
            columnType = sqlTypes.get(columnDefinition.dataType);
            if (columnType == null) {
                throw new TypeUnknownException(columnDefinition.dataType.getName());
            }
            columnSqlTypes.put(columnDefinition.columnName, new Tuple<>(columnDefinition, columnType));
        }
    }

    /**
     * guess the type of a given value and map it if a type could be guessed
     *
     * @param value the value to guess the type from and map
     * @return the mapped value
     * @throws TypeUnknownException if no type could be guessed
     */
    public Object guessAndMapType(Object value) throws TypeUnknownException, SQLType.ConvertException {

        if (value instanceof String) {
            if (!numberPattern.matcher((String)value).matches()) {
                try {
                    return new TimeStampSQLType().mappedValue(value);
                } catch(SQLType.ConvertException|IllegalArgumentException e) {
                    // no date, fall through to String
                }
            }
            return new StringSQLType().mappedValue(value);
        }
        else if (value instanceof Boolean) {
            return new BooleanSQLType().mappedValue(value);
        }
        else if (value instanceof Number) {
            if ((value instanceof Double) || (value instanceof Float) || (value instanceof BigDecimal)) {
                return new DoubleSQLType().mappedValue(value);
            } else if (((Number)value).longValue() < Integer.MIN_VALUE || Integer.MAX_VALUE < ((Number)value).longValue() ){
                return new LongSQLType().mappedValue(value);
            } else {
                return new IntegerSQLType().mappedValue(value);
            }

        } else if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> mappedCraty = ((Map<String, Object>)new CratySQLType().mappedValue(value));
            for (Map.Entry<String, Object> entry : mappedCraty.entrySet()) {
                mappedCraty.put(entry.getKey(), guessAndMapType(entry.getValue()));
            }
            return mappedCraty;
        }
        throw new TypeUnknownException(value.getClass().getSimpleName());
    }


    @SuppressWarnings("unchecked")
    @Override
    public Object mappedValue(String columnName, Object value)
            throws ColumnUnknownException, TypeUnknownException, ValidationException {
        Object converted;
        Tuple<ColumnDefinition, SQLType> columnAndType = columnSqlTypes.get(columnName);
        if (columnAndType == null) {
            columnAndType = getParentColumnAndType(columnName);
            if ((columnAndType != null && !columnAndType.v1().strict)
                        ||
                (columnAndType == null && metaDataExtractor.isDynamic())) {

                if (columnAndType != null && !columnAndType.v1().dynamic) {
                    return value;
                }
                try {
                    return guessAndMapType(value);
                } catch (SQLType.ConvertException e) {
                    throw new ValidationException(columnName, e.getMessage());
                }
            } else {
                throw new ColumnUnknownException(metaDataExtractor.getIndexName(), columnName);
            }

        } else {
            try {
                converted = columnAndType.v2().mappedValue(value);
            } catch (SQLType.ConvertException e) {
                throw new ValidationException(columnName, e.getMessage());
            }

            if (columnAndType.v2() instanceof CratySQLType) {
                for (Map.Entry<String, Object> entry : ((Map<String, Object>)converted).entrySet()) {

                    String joinedPath = Joiner.on('.').join(new String[]{columnName, entry.getKey()});
                    ((Map<String, Object>)converted).put(
                            entry.getKey(),
                            mappedValue(joinedPath, entry.getValue())
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
    private Tuple<ColumnDefinition, SQLType> getParentColumnAndType(String columnName) {
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
        Tuple<ColumnDefinition, SQLType> columnAndType = columnSqlTypes.get(columnName);
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
