package org.cratedb.lucene;

import org.cratedb.lucene.fields.LuceneField;
import org.cratedb.mapper.FieldMapper;

import java.util.LinkedHashMap;

public class LuceneFieldMapper extends LinkedHashMap<String, LuceneField> implements FieldMapper {

    @Override
    public Object mappedValue(String columnName, Object value) {
        LuceneField field = get(columnName);
        return field.mappedValue(value);
    }
}
