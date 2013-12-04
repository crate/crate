package org.cratedb.sql.types;

public abstract class NumericSQLType extends SQLType {

    protected abstract boolean checkRange(Number value);

    @Override
    protected Object doMapValue(Object value) throws ConvertException {
        Number number;
        try {
            number = (Number) value;
        } catch(ClassCastException e) {
            throw new ConvertException(typeName());
        }
        if (!checkRange(number)) {
            throw new ConvertException(typeName(), "out of bounds");
        }
        return mapNumber(number);
    }

    protected abstract Object mapNumber(Number value) throws ConvertException;

}
