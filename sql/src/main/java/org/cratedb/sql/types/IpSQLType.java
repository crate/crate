package org.cratedb.sql.types;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;

public class IpSQLType extends SQLType {

    public static final String NAME = "ip";

    @Override
    public String typeName() {
        return NAME;
    }

    @Override
    protected Object doMapValue(Object value) throws ConvertException {
        try {
            IpFieldMapper.ipToLong((String)value);
        } catch (ClassCastException e) {
            throw new ConvertException(typeName());
        } catch (ElasticSearchIllegalArgumentException e) {
            throw new ConvertException(typeName(), e.getMessage());
        }
        return value;
    }
}
