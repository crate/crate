package org.cratedb.mapper;

public interface FieldMapper {

    public Object mappedValue(String columnName, Object value);
}
