package org.cratedb.sql.types;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;

public class DoubleSQLType extends SQLType {
    public static final String NAME = "double";

    @Override
    public String mappingTypeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToDouble((BytesRef) value);
        }
        try {
            return Double.parseDouble(value.toString());
        } catch(Exception e) {
            throw new ConvertException(String.format("Invalid %s", mappingTypeName()));
        }
    }
}
