package org.cratedb.sql.types;

public abstract class NumericSQLType extends SQLType {

    protected abstract boolean checkRange(Number value);

    @Override
    protected Object doConvert(Object value) throws ConvertException {
        Number number;
        try {
            number = (Number) value;
        } catch(ClassCastException e) {
            throw new ConvertException(String.format("invalid %s", typeName()));
        }
        if (!checkRange(number)) {
            throw new ConvertException(String.format("%s out of bounds", typeName()));
        }
        return convertNumber(number);
    }

    protected abstract Object convertNumber(Number value) throws ConvertException;

}
