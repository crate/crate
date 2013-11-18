package org.cratedb.sql.types;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;

public class FloatSQLType extends SQLType {

    public static final String NAME = "float";

    @Override
    public String mappingTypeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }
        if (value instanceof BytesRef) {
            return Numbers.bytesToFloat((BytesRef) value);
        }
        try {
            return Float.parseFloat(value.toString());
        } catch(Exception e) {
            throw new ConvertException(String.format("Invalid %s", mappingTypeName()));
        }
    }


}
