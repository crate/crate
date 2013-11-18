package org.cratedb.sql.types;

public abstract class ScalarSQLType extends SQLType {

    protected abstract boolean checkRange(Number value);

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        if (!(value instanceof Number)) {
            throw new ConvertException(String.format("invalid %s", mappingTypeName()));
        }
        if (!checkRange((Number) value)) {
            throw new ConvertException(String.format("%s out of bounds", mappingTypeName()));
        }
        return convertNumber((Number) value);
    }

    protected abstract Object convertNumber(Number value) throws ConvertException;

}
