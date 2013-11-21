package org.cratedb.sql.types;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;

public class IpSQLType extends SQLType {

    public static final String NAME = "ip";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        try {
            return IpFieldMapper.ipToLong((String)value);
        } catch (Exception e) {
            throw new ConvertException(String.format("Invalid %s", typeName()));
        }
    }

    @Override
    public Object toDisplayValue(@Nullable Object value) {
        if (value != null && value instanceof Long) {
            try {
                return IpFieldMapper.longToIp((Long)value);
            } catch (Exception e) {
                // ignore
            }
        }
        return super.toDisplayValue(value);
    }
}
