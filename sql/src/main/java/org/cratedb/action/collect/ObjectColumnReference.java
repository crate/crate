package org.cratedb.action.collect;

import org.cratedb.DataType;

import java.util.Map;

public class ObjectColumnReference<ReturnType> extends ColumnReferenceCollectorExpression<Map<String, Object>> {

    public ObjectColumnReference(String columnName) {
        super(columnName);
    }

    @Override
    public Map<String, Object> evaluate() {
        //TODO: this returns null since we are called in aggregation collectors onnly vor now,
        // once this gets called from result columns it should be implemented
        return null;
    }

    @Override
    public DataType returnType() {
        return DataType.OBJECT;
    }

}
